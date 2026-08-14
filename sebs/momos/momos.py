import os
import subprocess
from typing import cast, Dict, List, Optional, Tuple, Type

import docker

from sebs.benchmark import Benchmark
from sebs.cache import Cache
from sebs.faas import System
from sebs.faas.function import Function, ExecutionResult, Trigger
from sebs.storage.minio import Minio
from sebs.storage.scylladb import ScyllaDB
from sebs.storage.resources import SelfHostedSystemResources
from sebs.utils import LoggingHandlers
from sebs.faas.config import Resources
from .config import MomosConfig
from .function import MomosFunction, MomosFunctionConfig
from .container import MomosContainer
from .triggers import CLITrigger, HTTPTrigger
from ..config import SeBSConfig


class Momos(System):
    _config: MomosConfig

    def __init__(
        self,
        system_config: SeBSConfig,
        config: MomosConfig,
        cache_client: Cache,
        docker_client: docker.client,
        logger_handlers: LoggingHandlers,
    ):
        super().__init__(
            system_config,
            cache_client,
            docker_client,
            SelfHostedSystemResources(
                "momos", config, cache_client, docker_client, logger_handlers
            ),
        )
        self._config = config
        self.logging_handlers = logger_handlers

        self._container_client = MomosContainer(
            self.system_config, self.config, self.docker_client, False
        )

        if self.config.docker_username:
            if self.config.docker_registry:
                docker_client.login(
                    username=self.config.docker_username,
                    password=self.config.docker_password,
                    registry=self.config.docker_registry,
                )
            else:
                docker_client.login(
                    username=self.config.docker_username,
                    password=self.config.docker_password,
                )

    def initialize(self, config: Dict[str, str] = {}, resource_prefix: Optional[str] = None):
        try:
            self.initialize_resources(select_prefix=resource_prefix)
        except RuntimeError as e:
            if "storage" in str(e).lower():
                self.logging.warning(f"Skipping storage initialization: {e}")
            else:
                raise

    @property
    def config(self) -> MomosConfig:
        return self._config

    @property
    def container_client(self):
        return self._container_client

    def shutdown(self) -> None:
        if hasattr(self, "storage") and self.config.remove_functions:
            try:
                pass
            except Exception:
                pass
        super().shutdown()

    @staticmethod
    def name() -> str:
        return "momos"

    @staticmethod
    def typename():
        return "Momos"

    @staticmethod
    def function_type() -> "Type[Function]":
        return MomosFunction

    def get_momos_cmd(self) -> List[str]:
        """Returns the base momos-cli command, including --gateway pointed
        at the real configured gateway (momos-cli's own default is
        http://localhost:4000, which won't reach a remote global node)."""
        return [self.config.momos_cli, "--gateway", self.config.gateway_url]

    def package_code(
        self,
        directory: str,
        language_name: str,
        language_version: str,
        architecture: str,
        benchmark: str,
        is_cached: bool,
    ) -> Tuple[str, float]:

        # Build image and register+push via momos-cli (handled inside container_client)
        _, image_uri, size_mb = self.container_client.build_base_image(
            directory, language_name, language_version, architecture, benchmark, is_cached
        )

        return directory, size_mb

    def storage_arguments(self, code_package: Benchmark) -> List[str]:
        envs: List[str] = []

        if self.config is None:
            return envs

        storage = None
        if self.system_resources is not None:
            try:
                storage = self.system_resources.get_storage()
            except RuntimeError:
                storage = None

        if storage is not None:
            storage_cfg = storage.config
            envs.extend(["--env", f"MINIO_STORAGE_CONNECTION_URL={storage_cfg.address}"])
            envs.extend(["--env", f"MINIO_ADDRESS={storage_cfg.address}"])
            envs.extend(["--env", f"MINIO_ACCESS_KEY={storage_cfg.access_key}"])
            envs.extend(["--env", f"MINIO_SECRET_KEY={storage_cfg.secret_key}"])

        if code_package.uses_nosql and self.system_resources is not None:
            try:
                nosql_storage = self.system_resources.get_nosql_storage()
                for key, value in nosql_storage.envs().items():
                    envs.extend(["--env", f"{key}={value}"])
                for original_name, actual_name in nosql_storage.get_tables(code_package.benchmark).items():
                    envs.extend(["--env", f"NOSQL_STORAGE_TABLE_{original_name}={actual_name}"])
            except RuntimeError:
                pass

        return envs

    def storage_env_dict(self) -> Dict[str, str]:
        """
        Object storage credentials as a plain dict, keyed exactly as the
        wrapper (__main__.py) expects to find them in the invocation
        payload -- momos-cli deploy has no env-injection flag, so these
        get merged into every invocation's payload by HTTPTrigger instead
        of being passed at deploy time.
        """
        env: Dict[str, str] = {}
        if self.system_resources is None:
            return env
        try:
            storage = self.system_resources.get_storage()
        except RuntimeError:
            return env
        storage_cfg = storage.config
        env["MINIO_STORAGE_CONNECTION_URL"] = storage_cfg.address
        env["MINIO_STORAGE_ACCESS_KEY"] = storage_cfg.access_key
        env["MINIO_STORAGE_SECRET_KEY"] = storage_cfg.secret_key
        return env

    @staticmethod
    def image_stem(image: str) -> str:
        """
        Mirrors momos-cli's own imageStem() exactly: strips a leading
        registry host (detected by containing '.' or ':' before the first
        '/') from an image reference. This is the name momos-cli actually
        registers the image under -- neither the full registry-qualified
        container_uri nor SeBS's own internal func_name match it, which
        was causing every invocation to 404 ("Image not found") even
        though deploy/register/push all succeeded.
        """
        parts = image.split("/", 1)
        if len(parts) == 2 and any(c in parts[0] for c in ".:"):
            return parts[1]
        return image

    def create_function(
        self,
        code_package: Benchmark,
        func_name: str,
        system_variant,
        container_uri: str,
    ) -> "MomosFunction":
        """
        'Deploy' in Momos means registering the image and making it invocable.
        The image was already pushed during package_code via momos deploy.
        We just record the function and attach triggers.
        """
        self.logging.info(f"Creating Momos function {func_name} → image {container_uri}.")

        function_cfg = MomosFunctionConfig.from_benchmark(code_package)
        function_cfg.docker_image = container_uri
        res = MomosFunction(func_name, code_package.benchmark, code_package.hash, function_cfg)

        # Both triggers must use the same identifier momos-cli actually
        # registered the image under -- the registry-stripped stem, not
        # container_uri (still registry-qualified) and not func_name
        # (SeBS's own internal name, never registered anywhere).
        registered_name = self.image_stem(container_uri)

        # HTTP trigger: POST /stream/invoke  (async, SSE result)
        invoke_url = f"{self.config.gateway_url.rstrip('/')}/stream/invoke"
        http_trigger = HTTPTrigger(registered_name, invoke_url, storage_env=self.storage_env_dict())
        http_trigger.logging_handlers = self.logging_handlers
        res.add_trigger(http_trigger)

        # CLI trigger: momos invoke --id <image_name>
        cli_trigger = CLITrigger(registered_name, self.get_momos_cmd())
        cli_trigger.logging_handlers = self.logging_handlers
        res.add_trigger(cli_trigger)

        return res

    def update_function(
        self,
        function: Function,
        code_package: Benchmark,
        system_variant,
        container_uri: str,
    ):
        """
        Re-deploy the image if it changed. momos deploy is idempotent
        (skips push if digest matches), so calling it again is safe.
        """
        self.logging.info(f"Updating Momos function {function.name} → image {container_uri}.")
        function = cast(MomosFunction, function)

        cmd = [
            *self.get_momos_cmd(),
            "deploy",
            "--image", container_uri,
        ]
        try:
            subprocess.run(cmd, check=True)
            function.config.docker_image = container_uri
        except FileNotFoundError as e:
            self.logging.error("Could not update Momos function - is momos-cli on your PATH?")
            raise RuntimeError(e)
        except subprocess.CalledProcessError as e:
            self.logging.error(f"momos deploy failed during update of {function.name}: {e}")
            raise RuntimeError(e)

    def update_function_configuration(self, function: Function, code_package: Benchmark):
        self.logging.info(f"Updating configuration of Momos function {function.name}.")
        try:
            self.update_function(function, code_package, code_package.system_variant, function.config.docker_image)
        except Exception as e:
            raise RuntimeError(e)

    def is_configuration_changed(self, cached_function: Function, benchmark: Benchmark) -> bool:
        changed = super().is_configuration_changed(cached_function, benchmark)

        storage = None
        if self.system_resources is not None:
            try:
                storage = cast(Minio, self.system_resources.get_storage())
            except RuntimeError:
                storage = None

        function = cast(MomosFunction, cached_function)
        if storage and function.config.object_storage != storage.config:
            self.logging.info("Updating function configuration due to changed storage configuration.")
            changed = True
            function.config.object_storage = storage.config

        nosql_storage = None
        if self.system_resources is not None:
            try:
                nosql_storage = cast(ScyllaDB, self.system_resources.get_nosql_storage())
            except RuntimeError:
                nosql_storage = None

        if nosql_storage and function.config.nosql_storage != nosql_storage.config:
            self.logging.info("Updating function configuration due to changed NoSQL storage configuration.")
            changed = True
            function.config.nosql_storage = nosql_storage.config

        return changed

    def default_function_name(self, code_package: Benchmark, resources: Optional[Resources] = None) -> str:
        resource_id = resources.resources_id if resources else self.config.docker_registry or "momos"
        resource_id = resource_id.replace(":", "-").replace("/", "-").lower()
        return f"sebs-{resource_id}-{code_package.benchmark}-{code_package.language_name}-{code_package.language_version}"

    def enforce_cold_start(self, functions: List[Function], code_package: Benchmark):
        raise NotImplementedError()

    def download_metrics(
        self,
        function_name: str,
        start_time: int,
        end_time: int,
        requests: Dict[str, ExecutionResult],
        metrics: dict,
    ):
        pass

    def create_trigger(self, function: Function, trigger_type: Trigger.TriggerType) -> Trigger:
        if trigger_type == Trigger.TriggerType.LIBRARY:
            return function.triggers(Trigger.TriggerType.LIBRARY)[0]
        elif trigger_type == Trigger.TriggerType.HTTP:
            registered_name = self.image_stem(function.config.docker_image)
            invoke_url = f"{self.config.gateway_url.rstrip('/')}/stream/invoke"
            trigger = HTTPTrigger(registered_name, invoke_url, storage_env=self.storage_env_dict())
            trigger.logging_handlers = self.logging_handlers
            function.add_trigger(trigger)
            self.cache_client.update_function(function)
            return trigger
        else:
            raise RuntimeError("Not supported!")

    def cached_function(self, function: Function):
        for trigger in function.triggers(Trigger.TriggerType.LIBRARY):
            trigger.logging_handlers = self.logging_handlers
            if isinstance(trigger, CLITrigger):
                trigger.momos_cmd = self.get_momos_cmd()
        for trigger in function.triggers(Trigger.TriggerType.HTTP):
            trigger.logging_handlers = self.logging_handlers
            if isinstance(trigger, HTTPTrigger):
                trigger.storage_env = self.storage_env_dict()

    def disable_rich_output(self):
        self.container_client.disable_rich_output = True