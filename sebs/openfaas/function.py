from __future__ import annotations

from typing import cast, Optional
from dataclasses import dataclass

from sebs.benchmark import Benchmark
from sebs.faas.function import Function, FunctionConfig, Runtime
from sebs.storage.config import MinioConfig, ScyllaDBConfig
from sebs.openfaas.triggers import CLITrigger, HTTPTrigger


@dataclass
class OpenFaaSFunctionConfig(FunctionConfig):
    docker_image: str = ""
    namespace: str = "openfaas-fn"
    object_storage: Optional[MinioConfig] = None
    nosql_storage: Optional[ScyllaDBConfig] = None

    @staticmethod
    def deserialize(data: dict) -> OpenFaaSFunctionConfig:
        keys = list(OpenFaaSFunctionConfig.__dataclass_fields__.keys())
        data = {k: v for k, v in data.items() if k in keys}
        data["runtime"] = Runtime.deserialize(data["runtime"])
        data["object_storage"] = MinioConfig.deserialize(data["object_storage"]) if data.get("object_storage") else None
        data["nosql_storage"] = ScyllaDBConfig.deserialize(data["nosql_storage"]) if data.get("nosql_storage") else None
        return OpenFaaSFunctionConfig(**data)

    def serialize(self) -> dict:
        return self.__dict__

    @staticmethod
    def from_benchmark(benchmark: Benchmark) -> OpenFaaSFunctionConfig:
        return super(OpenFaaSFunctionConfig, OpenFaaSFunctionConfig)._from_benchmark(
            benchmark, OpenFaaSFunctionConfig
        )


class OpenFaaSFunction(Function):
    def __init__(self, name: str, benchmark: str, code_package_hash: str, cfg: OpenFaaSFunctionConfig):
        super().__init__(benchmark, name, code_package_hash, cfg)

    @property
    def config(self) -> OpenFaaSFunctionConfig:
        return cast(OpenFaaSFunctionConfig, self._cfg)

    @staticmethod
    def typename() -> str:
        return "OpenFaaS.Function"

    def serialize(self) -> dict:
        return {**super().serialize(), "config": self._cfg.serialize()}

    @staticmethod
    def deserialize(cached_config: dict) -> OpenFaaSFunction:
        from sebs.faas.function import Trigger

        cfg = OpenFaaSFunctionConfig.deserialize(cached_config["config"])
        ret = OpenFaaSFunction(cached_config["name"], cached_config["benchmark"], cached_config["hash"], cfg)
        for trigger in cached_config["triggers"]:
            # Deserialize triggers
            ttype = trigger.get("type")
            if ttype == "Library" or ttype == "CLI":
                # CLI trigger for OpenFaaS
                cli_trigger = CLITrigger(trigger.get("name"), None, trigger.get("gateway"))
                ret.add_trigger(cli_trigger)
            elif ttype == "HTTP":
                # HTTP trigger for OpenFaaS
                http_trigger = HTTPTrigger(trigger.get("name"), trigger.get("url"))
                ret.add_trigger(http_trigger)
        return ret
