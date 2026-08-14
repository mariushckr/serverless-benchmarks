const path = require('path'), fs = require('fs');
const http = require('http');

async function main(args) {

  var minio_args = ["MINIO_STORAGE_CONNECTION_URL", "MINIO_STORAGE_ACCESS_KEY", "MINIO_STORAGE_SECRET_KEY"];
  minio_args.forEach(function(arg){
      if (arg in args) {
        process.env[arg] = args[arg];
        delete args[arg];
      }
  });

  // Set up NoSQL environment variables
  for (const key in args) {
    if (key.startsWith('NOSQL_STORAGE_')) {
      process.env[key] = args[key];
      delete args[key];
    }
  }

  var func = require('/function/function.js');
  var begin = Date.now() / 1000;
  var start = process.hrtime();
  var ret = await func.handler(args);
  var elapsed = process.hrtime(start);
  var end = Date.now() / 1000;
  var micro = elapsed[1] / 1e3 + elapsed[0] * 1e6;
  var is_cold = false;
  var fname = path.join('/tmp', 'cold_run');
  if (!fs.existsSync(fname)) {
    is_cold = true;
    fs.closeSync(fs.openSync(fname, 'w'));
  }

  // Get request ID from environment or generate one
  var request_id = process.env.X_CALL_ID || process.env.__OPENFAAS_INVOCATION_ID || 'unknown';

  return {
    begin: begin,
    end: end,
    compute_time: micro,
    results_time: 0,
    result: ret,
    request_id: request_id,
    is_cold: is_cold,
  };
}

exports.main = main;

// OpenFaaS entry point -- the container's own HTTP server is the invocation
// target directly (no separate watchdog binary in this Dockerfile), matching
// the same pattern the Python wrapper (Flask on :8080, POST /) already uses.
const PORT = process.env.PORT || 8080;

const server = http.createServer((req, res) => {
  if (req.method === 'GET' && req.url === '/_/health') {
    // Kubernetes readiness/liveness probe target. Without this, kubelet
    // kills and restarts the container repeatedly even though the app
    // itself is running fine -- same root cause confirmed on the Python
    // wrapper, applies identically here since it's the same faas-netes
    // Deployment/probe template regardless of language.
    res.writeHead(200);
    res.end();
    return;
  }

  if (req.method !== 'POST') {
    res.writeHead(405, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Method not allowed' }));
    return;
  }

  let body = '';
  req.on('data', (chunk) => { body += chunk; });
  req.on('end', async () => {
    try {
      const args = body ? JSON.parse(body) : {};
      const result = await main(args);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(result));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: String(err) }));
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Listening on port ${PORT}`);
});