import logging
import datetime
import os
import json
import sys
import time

import redis

logging.getLogger().setLevel(logging.INFO)

# The queue name is the image name, e.g. "myapp:latest"
# Injected by the cluster when it starts the container.

os.environ.setdefault("MINIO_STORAGE_CONNECTION_URL", "minio-sebs:9000")
os.environ.setdefault("MINIO_STORAGE_ACCESS_KEY", "minioadmin")
os.environ.setdefault("MINIO_STORAGE_SECRET_KEY", "minioadmin")

QUEUE_NAME    = os.environ["MOMOS_IMAGE_NAME"]   # e.g. "myapp:latest"
RESULTS_QUEUE = "stream-results"
REDIS_HOST    = os.environ.get("REDIS_HOST", "redis-momos")
REDIS_PORT    = int(os.environ.get("REDIS_PORT", "6379"))


def get_redis() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def handle_invocation(r: redis.Redis, raw: str):
    """
    Parse one invocation message, run the function, push the result.

    Invocation message shape (JSON):
        {
            "invocationId": "<uuid>",
            "imageId":      "<image-name>",
            "payload":      "<string>"   // may be a JSON string or plain text
        }

    Result message shape pushed to stream-results:
        {
            "invocationId": "<uuid>",
            "result":       { ... }      // full timing + function output
        }
    """
    try:
        invocation = json.loads(raw)
    except json.JSONDecodeError as e:
        logging.error(f"Could not parse invocation message: {e}  raw={raw!r}")
        return

    invocation_id = invocation.get("invocationId", "unknown")

    # Parse payload — may be a JSON string or a plain string
    raw_payload = invocation.get("payload", "{}")
    try:
        args = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
    except (json.JSONDecodeError, TypeError):
        args = {"payload": raw_payload}

    if not isinstance(args, dict):
        args = {"payload": args}

    # Inject invocation id so the function can reference it
    args["request-id"] = invocation_id

    begin = datetime.datetime.now()

    # Move storage credentials from payload into env vars
    for key in ["MINIO_STORAGE_CONNECTION_URL", "MINIO_STORAGE_ACCESS_KEY", "MINIO_STORAGE_SECRET_KEY"]:
        if key in args:
            os.environ[key] = args.pop(key)

    for key in list(args.keys()):
        if key.startswith("NOSQL_STORAGE_"):
            os.environ[key] = args.pop(key)

    # Cold-start detection
    is_cold = False
    cold_file = "/tmp/cold_run"
    if not os.path.exists(cold_file):
        is_cold = True
        open(cold_file, "a").close()

    logging.info(f"ENV CHECK: MINIO_STORAGE_CONNECTION_URL={os.environ.get('MINIO_STORAGE_CONNECTION_URL')} MINIO_STORAGE_ACCESS_KEY={os.environ.get('MINIO_STORAGE_ACCESS_KEY')}")
    logging.info(f"ARGS KEYS: {list(args.keys())}")

    try:
        sys.path.insert(0, "/")
        from function import handler
        ret = handler(args)
        end = datetime.datetime.now()

        results_time = (end - begin) / datetime.timedelta(microseconds=1)

        result_payload = {
            "invocationId": invocation_id,
            "result": {
                "begin":        begin.strftime("%s.%f"),
                "end":          end.strftime("%s.%f"),
                "request_id":   invocation_id,
                "results_time": results_time,
                "is_cold":      is_cold,
                "result":       ret,
            }
        }
    except Exception as e:
        end = datetime.datetime.now()
        results_time = (end - begin) / datetime.timedelta(microseconds=1)
        logging.error(f"Error in function execution: {e}", exc_info=True)

        result_payload = {
            "invocationId": invocation_id,
            "result": {
                "begin":        begin.strftime("%s.%f"),
                "end":          end.strftime("%s.%f"),
                "request_id":   invocation_id,
                "results_time": results_time,
                "is_cold":      is_cold,
                "result":       f"Error - invocation failed! Reason: {e}",
            }
        }

    r.rpush(RESULTS_QUEUE, json.dumps(result_payload))
    logging.info(f"[{invocation_id}] result pushed to {RESULTS_QUEUE}")


def is_shutdown_signal(raw: str) -> bool:
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return False
    return isinstance(msg, dict) and msg.get("type") == "shutdown"


def main():
    logging.info(f"Momos worker starting — listening on queue '{QUEUE_NAME}' "
                 f"(Redis {REDIS_HOST}:{REDIS_PORT})")
    r = get_redis()

    while True:
        try:
            # BLPOP blocks until a message arrives (timeout=0 = forever)
            item = r.blpop([f"{QUEUE_NAME}:control", QUEUE_NAME], timeout=0)
            if item is None:
                continue
            _, raw = item   # blpop returns (key, value)

            if is_shutdown_signal(raw):
                logging.info(f"Shutdown signal received on '{QUEUE_NAME}', exiting gracefully.")
                break

            logging.info(f"Received invocation from queue '{QUEUE_NAME}'")
            handle_invocation(r, raw)

        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            # TimeoutError here is a client-side socket read timeout, NOT a
            # real server-side problem -- health_check_interval causes
            # redis-py to impose an internal timeout on long blocking reads
            # so it can periodically send a health-check PING, and this
            # fires spuriously roughly every health_check_interval seconds
            # even when BLPOP's own timeout=0 means "wait forever" at the
            # protocol level. Previously this fell through to the generic
            # Exception handler below, which never refreshed the connection
            # -- any message arriving during one of these repeated
            # timeout/retry cycles was at real risk of being missed.
            logging.error(f"Redis connection issue: {e} — reconnecting in 2s")
            time.sleep(2)
            r = get_redis()

        except Exception as e:
            logging.error(f"Unexpected error in worker loop: {e}", exc_info=True)


if __name__ == "__main__":
    main()