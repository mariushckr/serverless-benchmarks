import os
import re
import subprocess
from typing import cast, Dict, List, Optional, Tuple

import docker

from sebs.benchmark import Benchmark
from sebs.cache import Cache
from sebs.faas import System
from sebs.faas.config import Resources
from sebs.faas.function import ExecutionResult, Function, Trigger
from sebs.faas.resources import SystemResources
from sebs.config import SeBSConfig
from sebs.sebs_types import Language
from sebs.storage.resources import SelfHostedSystemResources
from sebs.utils import LoggingHandlers

from .config import KnativeConfig
from .container import KnativeContainer
from .function import KnativeFunction, KnativeFunctionConfig
from .triggers import HTTPTrigger


class Knative(System):
    def __init__(
        self,
        system_config: SeBSConfig,
        config: KnativeConfig,
        cache_client: Cache,
        docker_client: docker.client.DockerClient,
        logger_handlers: LoggingHandlers,
    ):
        self._config = config
        system_resources = SelfHostedSystemResources(
            "knative", config, cache_client, docker_client, logger_handlers
        )
        super().__init__(system_config, cache_client, docker_client, system_resources)
        self.logging_handlers = logger_handlers

        self._container_client = KnativeContainer(
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
                    username=self.config.docker_username, password=self.config.docker_password
                )

    def shutdown(self) -> None:
        # Matches OpenFaaS's own stub -- actual function removal is
        # environment-specific and not implemented yet for either platform.
        if hasattr(self, "storage") and self.config.remove_functions:
            try:
                pass
            except Exception:
                pass
        super().shutdown()

    def initialize(self, config: Dict[str, str] = {}, resource_prefix: Optional[str] = None):
        try:
            self.initialize_resources(select_prefix=resource_prefix)
        except RuntimeError as e:
            if "storage" in str(e).lower():
                self.logging.warning(f"Skipping storage initialization: {e}")
            else:
                raise

    @staticmethod
    def name() -> str:
        return "knative"

    @staticmethod
    def typename() -> str:
        return "Knative"

    @staticmethod
    def function_type():
        return KnativeFunction

    @property
    def config(self) -> KnativeConfig:
        return self._config

    @property
    def container_client(self):
        return self._container_client

    def get_kn_cmd(self, kubeconfig_path: str) -> List[str]:
        """Returns the base kn command with KUBECONFIG set for a specific
        cluster -- kn has no --gateway-style flag; it's entirely driven by
        which kubeconfig/context is active, so each cluster needs its own
        explicit invocation, matching exactly why OpenFaaS needed
        clusterGateways: a stateless LB can't be used for deploy operations
        against multiple independent clusters."""
        return [self.config.kn_cli, "--kubeconfig", kubeconfig_path]

    def storage_env_args(self, code_package: Benchmark) -> List[str]:
        """
        --env KEY=VALUE pairs for storage credentials, passed at deploy
        time. Unlike Momos (which has no env-injection mechanism and needs
        credentials merged into the invocation payload instead), kn service
        create/update supports --env directly, so this is the same
        deploy-time pattern OpenFaaS uses -- no workaround needed.
        """
        args: List[str] = []
        storage = self.system_resources.get_storage()
        storage_cfg = storage.config
        args.extend(["--env", f"MINIO_STORAGE_CONNECTION_URL={storage_cfg.address}"])
        args.extend(["--env", f"MINIO_STORAGE_ACCESS_KEY={storage_cfg.access_key}"])
        args.extend(["--env", f"MINIO_STORAGE_SECRET_KEY={storage_cfg.secret_key}"])

        if code_package.uses_nosql:
            try:
                nosql_storage = self.system_resources.get_nosql_storage()
                for key, value in nosql_storage.envs().items():
                    args.extend(["--env", f"{key}={value}"])
                for original_name, actual_name in nosql_storage.get_tables(
                    code_package.benchmark
                ).items():
                    args.extend(["--env", f"NOSQL_STORAGE_TABLE_{original_name}={actual_name}"])
            except RuntimeError:
                pass

        return args

    @staticmethod
    def sanitize_service_name(raw: str) -> str:
        """
        Knative/Kubernetes Service names must be valid DNS-1035 labels:
        lowercase alphanumeric and '-' only, must start with a letter,
        max 63 chars. SeBS's normal function-naming convention (which can
        include dots, and for other platforms is sometimes derived from a
        registry address) is NOT valid here and must be sanitized, not
        just reused as-is.
        """
        s = raw.lower()
        s = re.sub(r"[^a-z0-9-]", "-", s)
        s = re.sub(r"-+", "-", s).strip("-")
        if not s or not s[0].isalpha():
            s = "fn-" + s
        return s[:63].rstrip("-")

    def default_function_name(
        self, code_package: Benchmark, resources: Optional[Resources] = None
    ) -> str:
        raw = (
            f"sebs-{code_package.benchmark}-{code_package.language_name}-"
            f"{code_package.language_version}"
        )
        return self.sanitize_service_name(raw)

    def host_header_for(self, func_name: str) -> str:
        return f"{func_name}.default.{self.config.domain_suffix}"

    def _deploy_to_all_clusters(self, func_name: str, image_uri: str, env_args: List[str]):
        """
        Deploy (create-or-update) on EVERY cluster's kubeconfig
        individually -- kn has no concept of a shared gateway to deploy
        through, and even if it did, the same round-robin-LB-splits-a-
        multi-step-operation risk that broke Momos's HTTP deploy path
        would apply here too. `kn service create` requires --force to
        behave as create-or-update -- without it, a second deploy against
        an already-existing service fails outright ("service already
        exists and no --force option was given"); confirmed by hitting
        this directly on a second run against the same func_name.
        """
        for cluster_id, kubeconfig_path in self.config.kubeconfigs.items():
            cmd = [
                *self.get_kn_cmd(kubeconfig_path),
                "service", "create", func_name,
                "--image", image_uri,
                "--port", "8080",
                "--force",
                *env_args,
            ]
            try:
                subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, check=True, text=True)
            except FileNotFoundError:
                raise RuntimeError(
                    f"kn binary not found at '{self.config.kn_cli}'. "
                    "Make sure the Knative CLI is installed and on your PATH."
                )
            except subprocess.CalledProcessError as e:
                self.logging.error(f"Cannot deploy function {func_name} to cluster {cluster_id}.")
                self.logging.error(f"stdout: {e.stdout}")
                self.logging.error(f"stderr: {e.stderr}")
                raise RuntimeError(f"kn service create on {cluster_id} failed: {e}")

    def package_code(
        self,
        directory: str,
        language_name: str,
        language_version: str,
        architecture: str,
        benchmark: str,
        is_cached: bool,
    ) -> Tuple[str, float]:
        """
        Dead code path for this platform: finalize_container_build() is
        not overridden (inherits the base's `return None`), so
        Benchmark.build() never actually calls this for container
        deployments -- matches the same situation confirmed for OpenFaaS.
        Implemented anyway for interface correctness.
        """
        _, image_uri, size_mb = self.container_client.build_base_image(
            directory, language_name, language_version, architecture, benchmark, is_cached
        )
        return directory, size_mb

    def create_function(
        self,
        code_package: Benchmark,
        func_name: str,
        system_variant,
        container_uri: str,
    ) -> "KnativeFunction":
        self.logging.info(f"Creating Knative function {func_name} → image {container_uri}.")

        env_args = self.storage_env_args(code_package)
        self._deploy_to_all_clusters(func_name, container_uri, env_args)

        function_cfg = KnativeFunctionConfig.from_benchmark(code_package)
        function_cfg.docker_image = container_uri
        res = KnativeFunction(func_name, code_package.benchmark, code_package.hash, function_cfg)

        host_header = self.host_header_for(func_name)
        http_trigger = HTTPTrigger(func_name, self.config.gateway_url, host_header)
        http_trigger.logging_handlers = self.logging_handlers
        res.add_trigger(http_trigger)

        return res

    def update_function(
        self,
        function: Function,
        code_package: Benchmark,
        system_variant,
        container_uri: str,
    ):
        self.logging.info(f"Updating Knative function {function.name} → image {container_uri}.")
        function = cast(KnativeFunction, function)

        env_args = self.storage_env_args(code_package)
        self._deploy_to_all_clusters(function.name, container_uri, env_args)
        function.config.docker_image = container_uri

    def update_function_configuration(self, cached_function: Function, benchmark: Benchmark):
        self.logging.info(f"Updating configuration of Knative function {cached_function.name}.")
        self.update_function(
            cached_function, benchmark, benchmark.system_variant, cached_function.config.docker_image
        )

    def is_configuration_changed(self, cached_function: Function, benchmark: Benchmark) -> bool:
        # Storage config isn't tracked as part of function config the way
        # OpenFaaS/Momos check it (no persisted storage-resource state
        # comparison here yet) -- always report unchanged beyond the base
        # class's own timeout/memory/runtime checks. Worth revisiting if
        # storage credentials ever rotate between runs against a cached
        # function.
        return super().is_configuration_changed(cached_function, benchmark)

    def create_trigger(self, function: Function, trigger_type: Trigger.TriggerType) -> Trigger:
        if trigger_type != Trigger.TriggerType.HTTP:
            raise NotImplementedError(
                "Knative only supports HTTP triggers -- there is no CLI-based "
                "invocation path (kn has no 'invoke' subcommand)."
            )
        host_header = self.host_header_for(function.name)
        trigger = HTTPTrigger(function.name, self.config.gateway_url, host_header)
        trigger.logging_handlers = self.logging_handlers
        function.add_trigger(trigger)
        self.cache_client.update_function(function)
        return trigger

    def cached_function(self, function: Function):
        for trigger in function.triggers(Trigger.TriggerType.HTTP):
            trigger.logging_handlers = self.logging_handlers
            if isinstance(trigger, HTTPTrigger):
                trigger.host_header = self.host_header_for(function.name)

    def enforce_cold_start(self, functions: List[Function], code_package: Benchmark):
        raise NotImplementedError(
            "Not implemented -- worth noting Knative's own KPA autoscaler "
            "already scales to zero on idle by default, so cold starts "
            "happen naturally and frequently without forcing them, unlike "
            "OpenFaaS/Momos which need an explicit mechanism for this."
        )

    def download_metrics(
        self,
        function_name: str,
        start_time: int,
        end_time: int,
        requests: Dict[str, ExecutionResult],
        metrics: dict,
    ):
        pass

    def disable_rich_output(self):
        self.container_client.disable_rich_output = True