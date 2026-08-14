from __future__ import annotations

from sebs.cache import Cache
from sebs.faas.config import Credentials, Resources, Config
from sebs.utils import LoggingHandlers
from sebs.storage.resources import SelfHostedResources
from typing import cast, Optional


class OpenFaaSCredentials(Credentials):
    @staticmethod
    def deserialize(config: dict, cache: Cache, handlers: LoggingHandlers) -> Credentials:
        return OpenFaaSCredentials()

    def serialize(self) -> dict:
        return {}


class OpenFaaSResources(SelfHostedResources):
    def __init__(
        self,
        registry: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        registry_updated: bool = False,
    ):
        super().__init__(name="openfaas")
        self._docker_registry = registry if registry != "" else None
        self._docker_username = username if username != "" else None
        self._docker_password = password if password != "" else None
        self._registry_updated = registry_updated
        self._storage_updated = False

    @staticmethod
    def typename() -> str:
        return "OpenFaaS.Resources"

    @property
    def docker_registry(self) -> Optional[str]:
        return self._docker_registry

    @property
    def docker_username(self) -> Optional[str]:
        return self._docker_username

    @property
    def docker_password(self) -> Optional[str]:
        return self._docker_password

    @property
    def storage_updated(self) -> bool:
        return self._storage_updated

    @property
    def registry_updated(self) -> bool:
        return self._registry_updated

    @staticmethod
    def initialize(res: Resources, dct: dict):
        ret = cast(OpenFaaSResources, res)
        ret._docker_registry = dct["registry"]
        ret._docker_username = dct["username"]
        ret._docker_password = dct["password"]

    @staticmethod
    def deserialize(config: dict, cache: Cache, handlers: LoggingHandlers) -> Resources:

        cached_config = cache.get_config("openfaas")
        ret = OpenFaaSResources()
        if cached_config:
            super(OpenFaaSResources, OpenFaaSResources).initialize(ret, cached_config["resources"])

        ret._deserialize(ret, config, cached_config)

        # Check for new config - overrides but check if it's different
        if "docker_registry" in config:

            OpenFaaSResources.initialize(ret, config["docker_registry"])
            ret.logging.info("Using user-provided Docker registry for OpenFaaS.")
            ret.logging_handlers = handlers

            # check if there has been an update
            if not (
                cached_config
                and "resources" in cached_config
                and "docker" in cached_config["resources"]
                and cached_config["resources"]["docker"] == config["docker_registry"]
            ):
                ret._registry_updated = True

        # Load cached values
        elif (
            cached_config
            and "resources" in cached_config
            and "docker" in cached_config["resources"]
        ):
            OpenFaaSResources.initialize(ret, cached_config["resources"]["docker"])
            ret.logging_handlers = handlers
            ret.logging.info("Using cached Docker registry for OpenFaaS")
        else:
            ret = OpenFaaSResources()
            ret.logging.info("Using default Docker registry for OpenFaaS.")
            ret.logging_handlers = handlers
            ret._registry_updated = True

        return ret

    def update_cache(self, cache: Cache):
        super().update_cache(cache)
        cache.update_config(val=self.docker_registry, keys=["openfaas", "resources", "docker", "registry"])
        cache.update_config(val=self.docker_username, keys=["openfaas", "resources", "docker", "username"])
        cache.update_config(val=self.docker_password, keys=["openfaas", "resources", "docker", "password"])

    def serialize(self) -> dict:
        out: dict = {
            **super().serialize(),
            "docker_registry": self.docker_registry,
            "docker_username": self.docker_username,
            "docker_password": self.docker_password,
        }
        return out


class OpenFaaSConfig(Config):
    name: str
    cache: Cache

    def __init__(self, config: dict, cache: Cache):
        super().__init__(name="openfaas")
        self._credentials = OpenFaaSCredentials()
        self._resources = OpenFaaSResources()
        # config keys: gatewayUrl, faasCli, removeFunctions, clusterGateways
        self.gateway_url = config.get("gatewayUrl", "http://127.0.0.1:8080")
        self.faas_cli = config.get("faasCli", "faas-cli")
        self.remove_functions = config.get("removeFunctions", True)
        # Individual per-cluster gateway addresses (bypassing the load
        # balancer), used ONLY for deploy/update operations. A round-robin
        # LB in front of multiple independent OpenFaaS clusters is correct
        # for invocation traffic, but faas-cli deploy internally makes
        # multiple separate HTTP requests (an existence check, then a
        # create-or-update) that can land on DIFFERENT clusters if routed
        # through the LB, causing spurious "already exists" conflicts --
        # confirmed by hitting this persistently, not just transiently.
        # Falls back to [gateway_url] (single-target behavior) if not set,
        # so this remains backward compatible with a single-cluster setup.
        self.cluster_gateways = config.get("clusterGateways", [self.gateway_url])
        self.cache = cache

    @property
    def credentials(self) -> OpenFaaSCredentials:
        return self._credentials

    @property
    def resources(self) -> OpenFaaSResources:
        return self._resources

    @property
    def docker_username(self) -> Optional[str]:
        return self._resources.docker_username

    @property
    def docker_password(self) -> Optional[str]:
        return self._resources.docker_password

    @property
    def docker_registry(self) -> Optional[str]:
        return self._resources.docker_registry

    @staticmethod
    def initialize(cfg: Config, dct: dict):
        pass

    def serialize(self) -> dict:
        return {
            "name": "openfaas",
            "gatewayUrl": self.gateway_url,
            "faasCli": self.faas_cli,
            "removeFunctions": self.remove_functions,
            "clusterGateways": self.cluster_gateways,
            "credentials": self._credentials.serialize(),
            "resources": self._resources.serialize(),
        }

    @staticmethod
    def deserialize(config: dict, cache: Cache, handlers: LoggingHandlers) -> Config:
        cached_config = cache.get_config("openfaas")
        resources = cast(OpenFaaSResources, OpenFaaSResources.deserialize(config, cache, handlers))

        res = OpenFaaSConfig(config, cache)
        res.logging_handlers = handlers
        res._resources = resources
        return res

    def update_cache(self, cache: Cache):
        cache.update_config(val=self.gateway_url, keys=["openfaas", "gatewayUrl"])
        cache.update_config(val=self.faas_cli, keys=["openfaas", "faasCli"])
        self.resources.update_cache(cache)