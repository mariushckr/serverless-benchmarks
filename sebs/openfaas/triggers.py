import concurrent.futures
import datetime
import json
import subprocess
import time
from typing import Dict, List, Optional

from sebs.faas.function import ExecutionResult, Trigger


class CLITrigger(Trigger):
    def __init__(self, fname: str, faas_cmd: Optional[List[str]] = None, gateway: Optional[str] = None):
        super().__init__()
        self.fname = fname
        self.gateway = gateway
        self._faas_cmd = [*faas_cmd, "invoke", self.fname] if faas_cmd else None

    @staticmethod
    def trigger_type() -> "Trigger.TriggerType":
        return Trigger.TriggerType.LIBRARY

    @property
    def faas_cmd(self) -> List[str]:
        assert self._faas_cmd
        return self._faas_cmd

    @faas_cmd.setter
    def faas_cmd(self, faas_cmd: List[str]):
        self._faas_cmd = [*faas_cmd, "invoke", self.fname]

    def sync_invoke(self, payload: dict) -> ExecutionResult:
        command = [*self.faas_cmd]
        if self.gateway:
            command += ["--gateway", self.gateway]
        error = None
        try:
            begin = datetime.datetime.now()
            proc = subprocess.run(
                command,
                input=json.dumps(payload),
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
            self.logging.error(f"Invocation of {self.fname} failed!")
            if isinstance(error, subprocess.CalledProcessError):
                self.logging.error(f"  stderr: {error.stderr if hasattr(error, 'stderr') else 'N/A'}")
                self.logging.error(f"  stdout: {error.stdout if hasattr(error, 'stdout') else 'N/A'}")
            faas_result.stats.failure = True
            return faas_result

        return_content = json.loads(parsed_response)
        faas_result.parse_benchmark_output(return_content)
        return faas_result

    def async_invoke(self, payload: dict) -> concurrent.futures.Future:
        pool = concurrent.futures.ThreadPoolExecutor()
        fut = pool.submit(self.sync_invoke, payload)
        return fut

    def serialize(self) -> dict:
        return {"type": "CLI", "name": self.fname, "gateway": self.gateway}

    @staticmethod
    def deserialize(obj: dict) -> Trigger:
        return CLITrigger(obj["name"], None, obj.get("gateway"))

    @staticmethod
    def typename() -> str:
        return "OpenFaaS.CLITrigger"


class HTTPTrigger(Trigger):
    def __init__(self, fname: str, url: str):
        super().__init__()
        self.fname = fname
        self.url = url

    @staticmethod
    def typename() -> str:
        return "OpenFaaS.HTTPTrigger"

    @staticmethod
    def trigger_type() -> Trigger.TriggerType:
        return Trigger.TriggerType.HTTP

    def sync_invoke(self, payload: dict) -> ExecutionResult:
        self.logging.debug(f"Invoke function {self.url}")
        return self._http_invoke(payload, self.url, False)

    def async_invoke(self, payload: dict) -> concurrent.futures.Future:
        pool = concurrent.futures.ThreadPoolExecutor()
        fut = pool.submit(self.sync_invoke, payload)
        return fut

    def serialize(self) -> dict:
        return {"type": "HTTP", "name": self.fname, "url": self.url}

    @staticmethod
    def deserialize(obj: dict) -> Trigger:
        return HTTPTrigger(obj["name"], obj["url"])
