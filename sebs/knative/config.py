from typing import Optional, cast

from sebs.cache import Cache
from sebs.faas.config import Config, Resources
from sebs.storage.resources import SelfHostedResources
from sebs.utils import LoggingHandlers


class KnativeCredentials:
    """Knative has no separate credentials concept beyond kubeconfig access
    and Docker registry auth (handled via KnativeResources), so this exists
    purely for interface symmetry with other platforms."""

    def serialize(self) -> dict:
        return {}


class KnativeResources(SelfHostedResources):
    def __init__(
        self,
        docker_registry: Optional[str] = None,
        docker_username: Optional[str] = None,
        docker_password: Optional[str] = None,
        registry_updated: bool = False,
    ):
        super().__init__(name="knative")
        self._docker_registry = docker_registry if docker_registry != "" else None
        self._docker_username = docker_username
        self._docker_password = docker_password
        self._registry_updated = registry_updated

    @property
    def docker_registry(self) -> Optional[str]:
        return self._docker_registry

    @property
    def docker_username(self) -> Optional[str]:
        return self._docker_username

    @property
    def docker_password(self) -> Optional[str]:
        return self._docker_password

    @staticmethod
    def initialize(res: "KnativeResources", dct: dict):
        res._docker_registry = dct.get("registry")
        res._docker_username = dct.get("username")
        res._docker_password = dct.get("password")

    def serialize(self) -> dict:
        out = super().serialize()
        out["docker_registry"] = self._docker_registry
        out["docker_username"] = self._docker_username
        return out

    @staticmethod
    def deserialize(config: dict, cache: Cache, handlers: LoggingHandlers) -> Resources:
        cached_config = cache.get_config("knative")
        ret = KnativeResources()

        if cached_config:
            KnativeResources.initialize(ret, cached_config.get("resources", {}))
        SelfHostedResources._deserialize(ret, config, cached_config)

        if "docker_registry" in config:
            KnativeResources.initialize(ret, config["docker_registry"])
            ret.logging.info("Using user-provided Docker registry for Knative.")
            ret._registry_updated = True
        elif cached_config and cached_config.get("resources", {}).get("docker_registry"):
            ret.logging.info("Using cached Docker registry for Knative.")
        else:
            ret.logging.info("Using default Docker registry for Knative.")
            ret._registry_updated = True

        ret.logging_handlers = handlers
        return ret

    def update_cache(self, cache: Cache) -> None:
        super().update_cache(cache)
        cache.update_config(
            val=self._docker_registry, keys=["knative", "resources", "docker_registry", "registry"]
        )
        cache.update_config(
            val=self._docker_username, keys=["knative", "resources", "docker_registry", "username"]
        )


class KnativeConfig(Config):
    name: str
    cache: Cache

    def __init__(self, config: dict, cache: Cache):
        super().__init__(name="knative")
        self._credentials = KnativeCredentials()
        self._resources = KnativeResources()

        # gatewayUrl: the load-balanced address invocations go through --
        # this is what HTTPTrigger uses, and is genuinely just an HTTP
        # reverse proxy in front of both clusters' Kourier gateways.
        self.gateway_url = config.get("gatewayUrl", "http://127.0.0.1:8080")

        # kubeconfigs: deploy operations CANNOT go through gatewayUrl at
        # all -- it only proxies HTTP invocation traffic, not the
        # Kubernetes API. Each cluster needs its own kubeconfig, deployed
        # to individually, for the same underlying reason OpenFaaS needed
        # clusterGateways: a round-robin LB in front of independent
        # clusters can't be used for anything beyond simple HTTP proxying.
        self.kubeconfigs = config.get("kubeconfigs", {})

        # sslip.io domain suffix used to construct the Host header for
        # invocation, e.g. "192.168.58.10.sslip.io" -- matches whatever
        # the knative-infra playbook configured via config-domain.
        self.domain_suffix = config.get("domainSuffix", "")

        self.kn_cli = config.get("knCli", "kn")
        self.remove_functions = config.get("removeFunctions", True)
        self.cache = cache

    @property
    def credentials(self) -> KnativeCredentials:
        return self._credentials

    @property
    def resources(self) -> KnativeResources:
        return self._resources

    @property
    def docker_registry(self) -> Optional[str]:
        return self._resources.docker_registry

    @property
    def docker_username(self) -> Optional[str]:
        return self._resources.docker_username

    @property
    def docker_password(self) -> Optional[str]:
        return self._resources.docker_password

    def serialize(self) -> dict:
        return {
            "name": "knative",
            "gatewayUrl": self.gateway_url,
            "kubeconfigs": self.kubeconfigs,
            "domainSuffix": self.domain_suffix,
            "knCli": self.kn_cli,
            "removeFunctions": self.remove_functions,
            "credentials": self._credentials.serialize(),
            "resources": self._resources.serialize(),
        }

    @staticmethod
    def initialize(cfg: "KnativeConfig", dct: dict):
        pass

    def update_cache(self, cache: Cache):
        cache.update_config(val=self.gateway_url, keys=["knative", "gatewayUrl"])
        cache.update_config(val=self.kubeconfigs, keys=["knative", "kubeconfigs"])
        cache.update_config(val=self.domain_suffix, keys=["knative", "domainSuffix"])
        cache.update_config(val=self.kn_cli, keys=["knative", "knCli"])
        self.resources.update_cache(cache)

    @staticmethod
    def deserialize(config: dict, cache: Cache, handlers: LoggingHandlers) -> Config:
        cached_config = cache.get_config("knative")
        resources = cast(KnativeResources, KnativeResources.deserialize(config, cache, handlers))

        res = KnativeConfig(config, cache)
        res.logging_handlers = handlers
        res._resources = resources
        return res
