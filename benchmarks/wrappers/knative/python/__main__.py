import datetime
import json
import os
import socket
import time
import uuid

from flask import Flask, request, jsonify

app = Flask(__name__)

PORT = int(os.environ.get("PORT", 8080))
COLD_MARKER = "/tmp/cold_run"


def is_cold_start() -> bool:
    if os.path.exists(COLD_MARKER):
        return False
    open(COLD_MARKER, "w").close()
    return True


@app.route("/_/health", methods=["GET"])
def health():
    """Defensive addition, not confirmed strictly required for Knative's
    default probe behavior (which may just be a TCP check) -- added
    proactively given the exact same missing route caused a silent
    crash-loop on OpenFaaS. Cheap to include either way."""
    return "", 200


@app.route("/", methods=["POST"])
def main():
    begin = datetime.datetime.now()
    cold = is_cold_start()

    args = request.get_json(force=True, silent=True) or {}
    request_id = args.pop("request-id", str(uuid.uuid4()))

    # Storage credentials arrive as deploy-time env vars (kn service
    # create/update --env), already set in os.environ by the time this
    # process starts -- no payload extraction needed, unlike Momos.

    from function import handler  # noqa: E402  (benchmark code, copied in at build time)

    fn_begin = time.time()
    try:
        result = handler(args)
        status = "ok"
    except Exception as e:
        result = {"error": str(e)}
        status = "error"
    fn_end = time.time()

    end = datetime.datetime.now()

    output = {
        "begin": str(begin.timestamp()),
        "end": str(end.timestamp()),
        "request_id": request_id,
        "results_time": (fn_end - fn_begin) * 1_000_000,
        "is_cold": cold,
        "result": {"result": result, "status": status},
    }

    return jsonify(output)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)