import concurrent.futures
import datetime
import json
from typing import Dict, Optional

from sebs.faas.function import ExecutionResult, Trigger


class HTTPTrigger(Trigger):
    """
    Invokes a Knative Service via the load-balanced Kourier gateway.
    POST <gatewayUrl>/  with Host: <service-name>.default.<domain_suffix>
    -> synchronous response, no polling needed (unlike Momos's async/SSE
    flow -- Knative Services are plain request/response HTTP).

    Storage credentials are passed at DEPLOY time as container env vars
    (via `kn service create/update --env`), not merged into the payload
    the way Momos needed -- Knative's deploy model supports env vars
    directly, so there's no equivalent workaround required here.
    """

    def __init__(self, function_name: str, gateway_url: str, host_header: str):
        super().__init__()
        self.function_name = function_name
        self.gateway_url = gateway_url.rstrip("/")
        self.host_header = host_header

    @staticmethod
    def typename() -> str:
        return "Knative.HTTPTrigger"

    @staticmethod
    def trigger_type() -> Trigger.TriggerType:
        return Trigger.TriggerType.HTTP

    def sync_invoke(self, payload: dict) -> ExecutionResult:
        import requests

        self.logging.debug(
            f"Invoke function {self.function_name} via {self.gateway_url} "
            f"(Host: {self.host_header})"
        )

        begin = datetime.datetime.now()
        try:
            resp = requests.post(
                self.gateway_url,
                json=payload,
                headers={"Host": self.host_header},
                timeout=120,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            end = datetime.datetime.now()
            self.logging.error(f"Invocation failed for {self.function_name}: {e}")
            faas_result = ExecutionResult.from_times(begin, end)
            faas_result.stats.failure = True
            return faas_result

        end = datetime.datetime.now()
        faas_result = ExecutionResult.from_times(begin, end)

        # Unlike Momos, this wrapper puts is_cold/begin/end at the TOP
        # level of the response, with "result" only wrapping the actual
        # benchmark output -- parse_benchmark_output needs those top-level
        # fields directly, not one level deeper.
        faas_result.parse_benchmark_output(data)
        faas_result.request_id = data.get("request_id", "")
        return faas_result

    def async_invoke(self, payload: dict) -> concurrent.futures.Future:
        pool = concurrent.futures.ThreadPoolExecutor()
        fut = pool.submit(self.sync_invoke, payload)
        return fut

    def serialize(self) -> dict:
        return {
            "type": "HTTP",
            "name": self.function_name,
            "gateway_url": self.gateway_url,
            "host_header": self.host_header,
        }

    @staticmethod
    def deserialize(obj: dict) -> Trigger:
        return HTTPTrigger(obj["name"], obj["gateway_url"], obj["host_header"])