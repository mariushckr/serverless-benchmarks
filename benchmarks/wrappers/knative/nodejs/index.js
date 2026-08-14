const http = require('http');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const COLD_MARKER = path.join('/tmp', 'cold_run');

function isColdStart() {
  if (fs.existsSync(COLD_MARKER)) {
    return false;
  }
  fs.closeSync(fs.openSync(COLD_MARKER, 'w'));
  return true;
}

const server = http.createServer((req, res) => {
  if (req.method === 'GET' && req.url === '/_/health') {
    // Defensive addition, not confirmed strictly required for Knative's
    // default probe behavior -- added proactively given the same missing
    // route caused a silent crash-loop on OpenFaaS.
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
    const begin = Date.now() / 1000;
    const cold = isColdStart();

    let args = {};
    try {
      args = body ? JSON.parse(body) : {};
    } catch (e) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Invalid JSON payload' }));
      return;
    }

    const requestId = args['request-id'] || crypto.randomUUID();
    delete args['request-id'];

    // Storage credentials arrive as deploy-time env vars, already set in
    // process.env by the time this process starts -- no payload
    // extraction needed here, unlike Momos.

    let result, status;
    const fnStart = process.hrtime();
    try {
      const func = require('/function/function.js');
      result = await func.handler(args);
      status = 'ok';
    } catch (e) {
      result = { error: e.message };
      status = 'error';
    }
    const fnElapsed = process.hrtime(fnStart);
    const resultsTimeMicro = fnElapsed[0] * 1e6 + fnElapsed[1] / 1e3;

    const end = Date.now() / 1000;

    const output = {
      begin: String(begin),
      end: String(end),
      request_id: requestId,
      results_time: resultsTimeMicro,
      is_cold: cold,
      result: { result: result, status: status },
    };

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(output));
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Knative worker listening on port ${PORT}`);
});
