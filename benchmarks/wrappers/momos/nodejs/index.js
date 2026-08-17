'use strict';
const path = require('path');
const fs   = require('fs');
const redis = require('redis');

// The queue name is the image name injected by the cluster, e.g. "myapp:latest"
const QUEUE_NAME    = process.env.MOMOS_IMAGE_NAME;
const RESULTS_QUEUE = 'stream-results';
const REDIS_HOST    = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT    = parseInt(process.env.REDIS_PORT || '6379', 10);

if (!QUEUE_NAME) {
  console.error('MOMOS_IMAGE_NAME environment variable is not set');
  process.exit(1);
}

// ── Redis client setup ────────────────────────────────────────────────────────
// Using two clients: one blocking (BLPOP subscriber), one for RPUSH results.
const subscriber = redis.createClient({ socket: { host: REDIS_HOST, port: REDIS_PORT } });
const publisher  = redis.createClient({ socket: { host: REDIS_HOST, port: REDIS_PORT } });

subscriber.on('error', (err) => console.error('Redis subscriber error:', err));
publisher.on('error',  (err) => console.error('Redis publisher error:', err));

// ── Cold-start detection ──────────────────────────────────────────────────────
const coldFile = path.join('/tmp', 'cold_run');
let isCold = !fs.existsSync(coldFile);
if (isCold) {
  fs.closeSync(fs.openSync(coldFile, 'w'));
}

// ── Invocation handler ────────────────────────────────────────────────────────
async function handleInvocation(raw) {
  let invocation;
  try {
    invocation = JSON.parse(raw);
  } catch (e) {
    console.error('Could not parse invocation message:', e, 'raw:', raw);
    return;
  }

  const invocationId = invocation.invocationId || 'unknown';

  // Parse payload — may be a JSON string, a plain string, or already an object
  let args = {};
  const rawPayload = invocation.payload || '{}';
  try {
    const parsed = typeof rawPayload === 'string' ? JSON.parse(rawPayload) : rawPayload;
    args = typeof parsed === 'object' && parsed !== null ? parsed : { payload: parsed };
  } catch (_) {
    args = { payload: rawPayload };
  }

  args['request-id'] = invocationId;

  // Move storage credentials from args into process.env
  const minioKeys = ['MINIO_STORAGE_CONNECTION_URL', 'MINIO_STORAGE_ACCESS_KEY', 'MINIO_STORAGE_SECRET_KEY'];
  minioKeys.forEach((key) => {
    if (key in args) {
      process.env[key] = args[key];
      delete args[key];
    }
  });
  Object.keys(args).forEach((key) => {
    if (key.startsWith('NOSQL_STORAGE_')) {
      process.env[key] = args[key];
      delete args[key];
    }
  });

  const func  = require('/function/function.js');
  const begin = Date.now() / 1000;
  const start = process.hrtime();

  let ret, error;
  try {
    ret = await func.handler(args);
  } catch (e) {
    error = e;
  }

  const elapsed     = process.hrtime(start);
  const end         = Date.now() / 1000;
  const computeTime = elapsed[1] / 1e3 + elapsed[0] * 1e6;

  const thisCold = isCold;
  isCold = false; // only first invocation in this container lifetime is cold

  const resultPayload = {
    invocationId,
    result: error
      ? {
          begin,
          end,
          compute_time: computeTime,
          results_time: 0,
          request_id:   invocationId,
          is_cold:      thisCold,
          result:       `Error - invocation failed! Reason: ${error.message}`,
        }
      : {
          begin,
          end,
          compute_time: computeTime,
          results_time: 0,
          request_id:   invocationId,
          is_cold:      thisCold,
          result:       ret,
        },
  };

  await publisher.rPush(RESULTS_QUEUE, JSON.stringify(resultPayload));
  console.log(`[${invocationId}] result pushed to ${RESULTS_QUEUE}`);
}

// ── Shutdown signal detection ─────────────────────────────────────────────────
function isShutdownSignal(raw) {
  try {
    const msg = JSON.parse(raw);
    return typeof msg === 'object' && msg !== null && msg.type === 'shutdown';
  } catch (_) {
    return false;
  }
}

// ── Main worker loop ──────────────────────────────────────────────────────────
async function main() {
  await subscriber.connect();
  await publisher.connect();

  console.log(`Momos worker starting — listening on queue '${QUEUE_NAME}' (Redis ${REDIS_HOST}:${REDIS_PORT})`);

  while (true) {
    try {
      // BLPOP blocks until a message arrives on either key; returns { key, element }.
      // Control key listed first so a shutdown signal never gets stuck
      // behind a continuously refilling work queue.
      const item = await subscriber.blPop([`${QUEUE_NAME}:control`, QUEUE_NAME], 0);
      if (!item) continue;

      if (isShutdownSignal(item.element)) {
        console.log(`Shutdown signal received on '${QUEUE_NAME}', exiting gracefully.`);
        await subscriber.quit();
        await publisher.quit();
        process.exit(0);
      }

      console.log(`Received invocation from queue '${QUEUE_NAME}'`);
      await handleInvocation(item.element);
    } catch (err) {
      console.error('Worker loop error:', err);
      // Brief pause before retrying on unexpected errors
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});