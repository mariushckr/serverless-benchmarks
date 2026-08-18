import concurrent.futures
import datetime
import json
import subprocess
from typing import Dict, List, Optional

from sebs.faas.function import ExecutionResult, Trigger

# Shared across every CLITrigger/HTTPTrigger instance and every invocation.
# max_workers raised from the original 64 -- confirmed via direct mpstat
# measurement that server-side cluster capacity (not this pool) was the
# actual bottleneck causing multi-hour backlog drains; this pool itself
# was never CPU-constrained on the host (each worker is mostly idle,
# blocked on I/O during its SSE wait). Raising this doesn't reduce total
# processing time -- that's still bounded by server-side capacity -- but
# it does mean less of the real backlog sits invisible in this pool's own
# internal queue and more of it genuinely reaches the system under test,
# which is a more honest reflection of actual queued demand.
_SHARED_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=256)


class CLITrigger(Trigger):
    """
    Invokes a Momos function via `momos invoke --id <image_name>`.

    The image name (e.g. "myapp:latest") is used as the function identifier,
    matching the momos-cli convention where --id is the image name rather
    than a UUID — so workers can pull directly from the registry without
    any extra lookup.
    """

    def __init__(self, image_name: str, momos_cmd: Optional[List[str]] = None, gateway: Optional[str] = None):
        super().__init__()
        self.image_name = image_name  # e.g. "myapp:latest"
        self.gateway = gateway
        # Build: momos invoke --id <image_name>
        self._momos_cmd = [*momos_cmd, "invoke", "--id", self.image_name] if momos_cmd else None

    @staticmethod
    def trigger_type() -> "Trigger.TriggerType":
        return Trigger.TriggerType.LIBRARY

    @property
    def momos_cmd(self) -> List[str]:
        assert self._momos_cmd
        return self._momos_cmd

    @momos_cmd.setter
    def momos_cmd(self, momos_cmd: List[str]):
        self._momos_cmd = [*momos_cmd, "invoke", "--id", self.image_name]

    def sync_invoke(self, payload: dict) -> ExecutionResult:
        command = [*self.momos_cmd]
        
        # Pass the payload as a JSON string via --payload
        command += ["--payload", json.dumps(payload)]

        error = None
        try:
            begin = datetime.datetime.now()
            proc = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                text=True,
            )
            end = datetime.datetime.now()
            parsed_response = proc.stdout
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            end = datetime.datetime.now()
            error = e

        faas_result = ExecutionResult.from_times(begin, end)
        if error is not None:
            self.logging.error(f"Invocation of {self.image_name} failed!")
            if isinstance(error, subprocess.CalledProcessError):
                self.logging.error(f"  stderr: {error.stderr if hasattr(error, 'stderr') else 'N/A'}")
                self.logging.error(f"  stdout: {error.stdout if hasattr(error, 'stdout') else 'N/A'}")
            faas_result.stats.failure = True
            return faas_result

        # momos invoke prints a result block; extract the JSON between the separator lines
        result_content = self._extract_result(parsed_response)
        return_content = json.loads(result_content)
        inner = return_content.get("result", return_content)
        faas_result.parse_benchmark_output(inner)
        faas_result.request_id = inner.get("request_id", return_content.get("invocationId", ""))
        return faas_result

    def _extract_result(self, output: str) -> str:
        """
        momos invoke output looks like:

            ── Result ──────────────────────────────────────────
            {"key": "value"}
            ────────────────────────────────────────────────────

        Extract just the JSON payload between the separator lines.
        Falls back to returning the raw output if the format isn't found.
        """
        lines = output.strip().splitlines()
        result_lines = []
        inside = False
        for line in lines:
            if line.startswith("── Result"):
                inside = True
                continue
            if inside and set(line.strip()) == {"─"}:
                break
            if inside:
                result_lines.append(line)
        if result_lines:
            return "\n".join(result_lines).strip()
        return output.strip()

    def async_invoke(self, payload: dict) -> concurrent.futures.Future:
        return _SHARED_EXECUTOR.submit(self.sync_invoke, payload)

    def serialize(self) -> dict:
        return {"type": "CLI", "name": self.image_name, "gateway": self.gateway}

    @staticmethod
    def deserialize(obj: dict) -> Trigger:
        return CLITrigger(obj["name"], None, obj.get("gateway"))

    @staticmethod
    def typename() -> str:
        return "Momos.CLITrigger"


class HTTPTrigger(Trigger):
    """
    Invokes a Momos function via the HTTP gateway endpoint.
    POST <gatewayUrl>/stream/invoke  with { imageId, payload }
    -> 202 { requestId, status: "pending" }
    then opens an SSE connection to GET <gatewayUrl>/invocations/<requestId>
    and waits specifically for a named "result" SSE event (confirmed against
    the real gateway's Api.java: Javalin SSE, sseClient.sendEvent("result", ...)).

    Storage credentials are merged directly into the invocation payload
    (not passed as deploy-time container env vars) -- momos-cli deploy has
    no env-injection flag, and the wrapper (__main__.py) already expects
    these exact keys to arrive in the payload and moves them to os.environ
    itself, so this is the natural place for them.
    """

    def __init__(self, image_name: str, url: str, storage_env: Optional[Dict[str, str]] = None):
        super().__init__()
        self.image_name = image_name
        self.url = url  # e.g. http://localhost:8080/stream/invoke
        # Derive the base gateway URL for the SSE endpoint from the invoke URL.
        if url.endswith("/stream/invoke"):
            self.gateway_base = url[: -len("/stream/invoke")]
        else:
            self.gateway_base = url.rsplit("/", 1)[0]
        self.storage_env = storage_env or {}

    @staticmethod
    def typename() -> str:
        return "Momos.HTTPTrigger"

    @staticmethod
    def trigger_type() -> Trigger.TriggerType:
        return Trigger.TriggerType.HTTP

    def dispatch(self, payload: dict):
        """
        Just the POST to /stream/invoke -- fast (milliseconds), meant to be
        called DIRECTLY in the scheduler's loop, never via async_invoke().
        This is the real "arrival" event at MOMOS; nothing about worker-pool
        backlog can delay it, since it never touches the shared executor.

        Returns (request_id, begin_timestamp) on success, or (None,
        begin_timestamp) with a failure already logged if the POST itself
        fails -- callers should check for None before calling
        wait_for_result().
        """
        import requests

        self.logging.debug(f"Invoke function {self.image_name} via {self.url}")
        full_payload = {**payload, **self.storage_env}
        begin = datetime.datetime.now()

        try:
            resp = requests.post(
                self.url,
                json={"imageId": self.image_name, "payload": json.dumps(full_payload)},
                timeout=10,
            )
            resp.raise_for_status()
            request_id = resp.json()["requestId"]
            return request_id, begin
        except Exception as e:
            self.logging.error(f"Failed to submit invocation for {self.image_name}: {e}")
            return None, begin

    def wait_for_result(self, request_id: Optional[str], begin: datetime.datetime) -> ExecutionResult:
        """
        The slow part -- opens the SSE stream and waits for the "result"
        event, up to 600s. Meant to run inside the worker pool via
        async_invoke_result(), where backlog is fine since real arrival at
        MOMOS has already genuinely happened in dispatch() before this was
        ever called.
        """
        import requests

        if request_id is None:
            # dispatch() itself already failed -- nothing to wait for.
            end = datetime.datetime.now()
            faas_result = ExecutionResult.from_times(begin, end)
            faas_result.stats.failure = True
            return faas_result

        result_data = None
        sse_url = f"{self.gateway_base}/invocations/{request_id}"
        try:
            with requests.get(
                sse_url, stream=True, timeout=600, headers={"Accept": "text/event-stream"}
            ) as r:
                r.raise_for_status()
                current_event = None
                data_lines: List[str] = []
                for line in r.iter_lines(decode_unicode=True):
                    if line is None:
                        continue
                    if line == "":
                        if current_event == "result":
                            result_data = "\n".join(data_lines)
                            break
                        current_event = None
                        data_lines = []
                        continue
                    if line.startswith("event:"):
                        current_event = line[len("event:"):].strip()
                    elif line.startswith("data:"):
                        data_lines.append(line[len("data:"):].strip())
        except Exception as e:
            end = datetime.datetime.now()
            self.logging.error(f"SSE stream failed for request {request_id}: {e}")
            faas_result = ExecutionResult.from_times(begin, end)
            faas_result.stats.failure = True
            return faas_result

        end = datetime.datetime.now()
        faas_result = ExecutionResult.from_times(begin, end)

        if result_data is None:
            self.logging.error(
                f"No 'result' event received for request {request_id} "
                "(SSE connection closed or timed out without one)"
            )
            faas_result.stats.failure = True
            return faas_result

        try:
            parsed = json.loads(result_data)
        except json.JSONDecodeError:
            self.logging.error(
                f"Result for {request_id} was not valid JSON: {result_data[:200]!r}. "
                "The gateway's result.toString() format may not be JSON-compatible."
            )
            faas_result.stats.failure = True
            return faas_result

        inner = parsed.get("result", parsed)
        faas_result.parse_benchmark_output(inner)
        faas_result.request_id = inner.get("request_id", request_id)
        return faas_result

    def sync_invoke(self, payload: dict) -> ExecutionResult:
        """Combined dispatch + wait, kept for anything still calling this
        directly (e.g. a single non-streamed invoke)."""
        request_id, begin = self.dispatch(payload)
        return self.wait_for_result(request_id, begin)

    def async_invoke_result(self, request_id: Optional[str], begin: datetime.datetime) -> concurrent.futures.Future:
        """Hands the SLOW part off to the shared pool -- safe to backlog,
        since dispatch() already happened synchronously before this is
        ever called."""
        return _SHARED_EXECUTOR.submit(self.wait_for_result, request_id, begin)

    def async_invoke(self, payload: dict) -> concurrent.futures.Future:
        return _SHARED_EXECUTOR.submit(self.sync_invoke, payload)

    def serialize(self) -> dict:
        return {"type": "HTTP", "name": self.image_name, "url": self.url}

    @staticmethod
    def deserialize(obj: dict) -> Trigger:
        # NOTE: storage_env is not persisted across cache serialization --
        # a trigger rehydrated from cache will have empty storage_env until
        # something re-sets it (e.g. cached_function() in momos.py).
        return HTTPTrigger(obj["name"], obj["url"])