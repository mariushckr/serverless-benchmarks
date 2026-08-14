from dataclasses import dataclass
from typing import cast, Optional

from sebs.benchmark import Benchmark
from sebs.faas.function import Function, FunctionConfig, Trigger
from .triggers import HTTPTrigger


@dataclass
class KnativeFunctionConfig(FunctionConfig):
    docker_image: str = ""

    @staticmethod
    def deserialize(data: dict) -> "KnativeFunctionConfig":
        keys = list(KnativeFunctionConfig.__dataclass_fields__.keys())
        data = {k: v for k, v in data.items() if k in keys}
        from sebs.faas.function import Runtime

        data["runtime"] = Runtime.deserialize(data["runtime"])
        return KnativeFunctionConfig(**data)

    def serialize(self) -> dict:
        return self.__dict__

    @staticmethod
    def from_benchmark(benchmark: Benchmark) -> "KnativeFunctionConfig":
        return super(KnativeFunctionConfig, KnativeFunctionConfig)._from_benchmark(
            benchmark, KnativeFunctionConfig
        )


class KnativeFunction(Function):
    def __init__(
        self, name: str, benchmark: str, code_hash: str, cfg: KnativeFunctionConfig
    ):
        super().__init__(benchmark, name, code_hash, cfg)

    @property
    def config(self) -> KnativeFunctionConfig:
        return cast(KnativeFunctionConfig, self._cfg)

    def serialize(self) -> dict:
        return {**super().serialize()}

    @staticmethod
    def deserialize(cached_config: dict) -> "KnativeFunction":
        cfg = KnativeFunctionConfig.deserialize(cached_config["config"])
        ret = KnativeFunction(
            cached_config["name"], cached_config["benchmark"], cached_config["hash"], cfg
        )
        for trigger in cached_config.get("triggers", []):
            trigger_type = {"HTTP": HTTPTrigger}.get(trigger["type"])
            if trigger_type is None:
                raise RuntimeError(f"Unknown trigger type {trigger['type']} for Knative function")
            ret.add_trigger(trigger_type.deserialize(trigger))
        return ret