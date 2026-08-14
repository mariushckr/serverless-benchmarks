import logging
import datetime
import os
import json
import sys
from flask import Flask, request

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
logging.getLogger().setLevel(logging.INFO)

def invoke_function(args):
    """Core function invocation logic"""
    begin = datetime.datetime.now()
    args['request-id'] = request.headers.get('X-Call-Id', 'unknown')
    args['income-timestamp'] = begin.timestamp()

    # Set up storage environment variables
    for arg in ["MINIO_STORAGE_CONNECTION_URL", "MINIO_STORAGE_ACCESS_KEY", "MINIO_STORAGE_SECRET_KEY"]:
        if arg in args:
            os.environ[arg] = args[arg]
            del args[arg]

    # Set up NoSQL environment variables
    key_list = list(args.keys())
    for arg in key_list:
        if 'NOSQL_STORAGE_' in arg:
            os.environ[arg] = args[arg]
            del args[arg]

    try:
        from function import handler
        ret = handler(args)
        end = datetime.datetime.now()
        logging.info("Function result: {}".format(ret))
        log_data = {"result": ret["result"]}
        if "measurement" in ret:
            log_data["measurement"] = ret["measurement"]

        results_time = (end - begin) / datetime.timedelta(microseconds=1)

        is_cold = False
        fname = "cold_run"
        if not os.path.exists(fname):
            is_cold = True
            open(fname, "a").close()

        return {
            "begin": begin.strftime("%s.%f"),
            "end": end.strftime("%s.%f"),
            "request_id": args.get('request-id', 'unknown'),
            "results_time": results_time,
            "is_cold": is_cold,
            "result": log_data,
        }
    except Exception as e:
        end = datetime.datetime.now()
        results_time = (end - begin) / datetime.timedelta(microseconds=1)
        logging.error(f"Error in function execution: {e}", exc_info=True)
        return {
            "begin": begin.strftime("%s.%f"),
            "end": end.strftime("%s.%f"),
            "request_id": args.get('request-id', 'unknown'),
            "results_time": results_time,
            "result": f"Error - invocation failed! Reason: {e}"
        }

@app.route('/_/health', methods=['GET'])
def health():
    """Kubernetes readiness/liveness probe target. Without this, kubelet
    kills and restarts the container repeatedly (CrashLoopBackOff) even
    though the app itself is running fine -- confirmed by hitting exactly
    this: clean Flask startup, then continuous 404s on this path, then
    repeated restarts."""
    return '', 200

@app.route('/', methods=['POST'])
def main():
    """OpenFaaS entry point"""
    try:
        if request.is_json:
            payload = request.get_json()
        else:
            payload = json.loads(request.data.decode('utf-8'))
        
        result = invoke_function(payload)
        return json.dumps(result), 200, {'Content-Type': 'application/json'}
    except Exception as e:
        logging.error(f"Error handling request: {e}", exc_info=True)
        return json.dumps({"error": str(e)}), 500, {'Content-Type': 'application/json'}

if __name__ == '__main__':
    # Get port from environment or default to 8080
    port = int(os.getenv('PORT', '8080'))
    app.run(host='0.0.0.0', port=port, threaded=True)