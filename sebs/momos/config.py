from __future__ import annotations

from sebs.cache import Cache
from sebs.faas.config import Credentials, Resources, Config
from sebs.utils import LoggingHandlers
from sebs.storage.resources import SelfHostedResources
from typing import cast, Optional


class MomosCredentials(Credentials):
    @staticmethod
    def deserialize(config: dict, cache: Cache, handlers: LoggingHandlers) -> Credentials:
        return MomosCredentials()

    def serialize(self) -> dict:
        return {}


class MomosResources(SelfHostedResources):
    def __init__(
        self,
        registry: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        registry_updated: bool = False,
    ):
        super().__init__(name="momos")
        self._docker_registry = registry if registry != "" else None
        self._docker_username = username if username != "" else None
        self._docker_password = password if password != "" else None
        self._registry_updated = registry_updated
        self._storage_updated = False

    @staticmethod
    def typename() -> str:
        return "Momos.Resources"

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
        ret = cast(MomosResources, res)
        ret._docker_registry = dct["registry"]
        ret._docker_username = dct["username"]
        ret._docker_password = dct["password"]

    @staticmethod
    def deserialize(config: dict, cache: Cache, handlers: LoggingHandlers) -> Resources:
        cached_config = cache.get_config("momos")
        ret = MomosResources()
        if cached_config:
            super(MomosResources, MomosResources).initialize(ret, cached_config["resources"])

        ret._deserialize(ret, config, cached_config)

        # Check for new config - overrides but check if it's different
        if "docker_registry" in config:
            MomosResources.initialize(ret, config["docker_registry"])
            ret.logging.info("Using user-provided Docker registry for Momos.")
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
            MomosResources.initialize(ret, cached_config["resources"]["docker"])
            ret.logging_handlers = handlers
            ret.logging.info("Using cached Docker registry for Momos.")
        else:
            ret = MomosResources()
            ret.logging.info("Using default Docker registry for Momos.")
            ret.logging_handlers = handlers
            ret._registry_updated = True

        return ret

    def update_cache(self, cache: Cache):
        super().update_cache(cache)
        cache.update_config(val=self.docker_registry, keys=["momos", "resources", "docker", "registry"])
        cache.update_config(val=self.docker_username, keys=["momos", "resources", "docker", "username"])
        cache.update_config(val=self.docker_password, keys=["momos", "resources", "docker", "password"])

    def serialize(self) -> dict:
        out: dict = {
            **super().serialize(),
            "docker_registry": self.docker_registry,
            "docker_username": self.docker_username,
            "docker_password": self.docker_password,
        }
        return out


class MomosConfig(Config):
    name: str
    cache: Cache

    def __init__(self, config: dict, cache: Cache):
        super().__init__(name="momos")
        self._credentials = MomosCredentials()
        self._resources = MomosResources()
        # config keys: gatewayUrl, momosCli, removeFunctions
        self.gateway_url = config.get("gatewayUrl", "http://127.0.0.1:8080")
        self.momos_cli = config.get("momosCli", "momos")
        self.remove_functions = config.get("removeFunctions", True)
        self.cache = cache

    @property
    def credentials(self) -> MomosCredentials:
        return self._credentials

    @property
    def resources(self) -> MomosResources:
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
            "name": "momos",
            "gatewayUrl": self.gateway_url,
            "momosCli": self.momos_cli,
            "removeFunctions": self.remove_functions,
            "credentials": self._credentials.serialize(),
            "resources": self._resources.serialize(),
        }

    @staticmethod
    def deserialize(config: dict, cache: Cache, handlers: LoggingHandlers) -> Config:
        cached_config = cache.get_config("momos")
        resources = cast(MomosResources, MomosResources.deserialize(config, cache, handlers))

        res = MomosConfig(config, cache)
        res.logging_handlers = handlers
        res._resources = resources
        return res

    def update_cache(self, cache: Cache):
        cache.update_config(val=self.gateway_url, keys=["momos", "gatewayUrl"])
        cache.update_config(val=self.momos_cli, keys=["momos", "momosCli"])
        self.resources.update_cache(cache)
