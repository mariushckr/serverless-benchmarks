import os
import subprocess
import hashlib
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
from .config import OpenFaaSConfig
from .function import OpenFaaSFunction, OpenFaaSFunctionConfig
from .container import OpenFaaSContainer
from .triggers import CLITrigger, HTTPTrigger
from ..config import SeBSConfig


class OpenFaaS(System):
    _config: OpenFaaSConfig

    def __init__(
        self,
        system_config: SeBSConfig,
        config: OpenFaaSConfig,
        cache_client: Cache,
        docker_client: docker.client,
        logger_handlers: LoggingHandlers,
    ):
        super().__init__(
            system_config,
            cache_client,
            docker_client,
            SelfHostedSystemResources(
                "openfaas", config, cache_client, docker_client, logger_handlers
            ),
        )
        self._config = config
        self.logging_handlers = logger_handlers

        self._container_client = OpenFaaSContainer(self.system_config, self.config, self.docker_client, False)

        if self.config.docker_username:
            if self.config.docker_registry:
                docker_client.login(username=self.config.docker_username, password=self.config.docker_password, registry=self.config.docker_registry)
            else:
                docker_client.login(username=self.config.docker_username, password=self.config.docker_password)

    def initialize(self, config: Dict[str, str] = {}, resource_prefix: Optional[str] = None):
        # OpenFaaS doesn't require traditional storage deployment in the same way
        # Skip initialize_resources if no storage is configured
        try:
            self.initialize_resources(select_prefix=resource_prefix)
        except RuntimeError as e:
            if "storage" in str(e).lower():
                self.logging.warning(f"Skipping storage initialization: {e}")
            else:
                raise

    @property
    def config(self) -> OpenFaaSConfig:
        return self._config

    @property
    def container_client(self):
        return self._container_client

    def shutdown(self) -> None:
        # Optionally remove functions and stop storage
        if hasattr(self, "storage") and self.config.remove_functions:
            try:
                # remove functions may be environment specific; leave for now
                pass
            except Exception:
                pass
        super().shutdown()

    @staticmethod
    def name() -> str:
        return "openfaas"

    @staticmethod
    def typename():
        return "OpenFaaS"

    @staticmethod
    def function_type() -> "Type[Function]":
        return OpenFaaSFunction

    def get_faas_cli_cmd(self) -> List[str]:
        cmd = [self.config.faas_cli]
        return cmd

    def package_code(
        self,
        directory: str,
        language_name: str,
        language_version: str,
        architecture: str,
        benchmark: str,
        is_cached: bool,
    ) -> Tuple[str, float]:

        # Build and push image using container client
        _, image_uri, size_mb = self.container_client.build_base_image(
            directory, language_name, language_version, architecture, benchmark, is_cached
        )

        return directory, size_mb

    def storage_arguments(self, code_package: Benchmark) -> List[str]:
        envs: List[str] = []

        if self.config is None:
            return envs

        # Safely get storage if available
        storage = None
        if self.system_resources is not None:
            try:
                storage = self.system_resources.get_storage()
            except RuntimeError:
                # Storage not available, continue without it
                storage = None

        if storage is not None:
            storage_cfg = storage.config
            envs.extend(["--env", f"MINIO_STORAGE_CONNECTION_URL={storage_cfg.address}"])
            envs.extend(["--env", f"MINIO_ADDRESS={storage_cfg.address}"])
            envs.extend(["--env", f"MINIO_ACCESS_KEY={storage_cfg.access_key}"])
            envs.extend(["--env", f"MINIO_SECRET_KEY={storage_cfg.secret_key}"])

        # Handle NoSQL storage if benchmark uses it
        if code_package.uses_nosql and self.system_resources is not None:
            try:
                nosql_storage = self.system_resources.get_nosql_storage()
                for key, value in nosql_storage.envs().items():
                    envs.extend(["--env", f"{key}={value}"])
                for original_name, actual_name in nosql_storage.get_tables(code_package.benchmark).items():
                    envs.extend(["--env", f"NOSQL_STORAGE_TABLE_{original_name}={actual_name}"])
            except RuntimeError:
                # NoSQL storage not available
                pass

        return envs

    def _deploy_to_all_clusters(self, func_name: str, image_uri: str, storage_env_args: List[str]):
        """
        Deploy (create-or-update) the function on EVERY cluster gateway
        individually, bypassing the load balancer. faas-cli deploy internally
        makes multiple separate HTTP requests (an existence check, then a
        create-or-update call) -- routing those through a round-robin LB in
        front of multiple INDEPENDENT clusters can split them across
        different clusters with different state, producing a persistent
        (not transient) "already exists" conflict. Confirmed by hitting
        exactly this repeatedly, including across retries with delays.
        Deploying to each cluster directly and explicitly sidesteps this
        entirely, and ensures the function is actually available on every
        cluster the LB might route an invocation to.

        Autoscaling labels: without these, every function deploys pinned at
        a single replica regardless of load -- confirmed empirically by
        watching `kubectl get deploy` stay at 1/1 throughout a real
        concurrent batch run. scale.max is capped at 5, matching OpenFaaS
        CE's documented per-function replica limit (confirmed via OpenFaaS's
        own docs); requesting more than that would just be silently unused
        headroom.
        """
        for cluster_gateway in self.config.cluster_gateways:
            cmd = [
                *self.get_faas_cli_cmd(), "deploy",
                "--image", image_uri,
                "--name", func_name,
                "--gateway", cluster_gateway,
                "--label", "com.openfaas.scale.min=1",
                "--label", "com.openfaas.scale.max=5",
                "--label", "com.openfaas.scale.factor=20",
                *storage_env_args,
            ]

            max_attempts = 3
            for attempt in range(1, max_attempts + 1):
                try:
                    subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, check=True, text=True)
                    break
                except FileNotFoundError:
                    self.logging.error("Could not deploy OpenFaaS function - is path to faas-cli correct?")
                    raise RuntimeError("Failed to access faas-cli binary")
                except subprocess.CalledProcessError as e:
                    already_exists = "already exists" in (e.stdout or "") + (e.stderr or "")
                    if already_exists and attempt < max_attempts:
                        self.logging.warning(
                            f"Transient 'already exists' conflict deploying {func_name} "
                            f"to {cluster_gateway} (attempt {attempt}/{max_attempts}), retrying in 3s..."
                        )
                        import time
                        time.sleep(3)
                        continue
                    self.logging.error(f"Cannot deploy function {func_name} to {cluster_gateway}.")
                    self.logging.error(f"stdout: {e.stdout if hasattr(e, 'stdout') else 'N/A'}")
                    self.logging.error(f"stderr: {e.stderr if hasattr(e, 'stderr') else 'N/A'}")
                    raise RuntimeError(f"faas-cli deploy to {cluster_gateway} failed: {e}")

    def create_function(
        self,
        code_package: Benchmark,
        func_name: str,
        system_variant,
        container_uri: str,
    ) -> "OpenFaaSFunction":
        self.logging.info("Creating function as a service in OpenFaaS.")
        image_uri = container_uri

        storage_env_args = self.storage_arguments(code_package)
        self._deploy_to_all_clusters(func_name, image_uri, storage_env_args)

        function_cfg = OpenFaaSFunctionConfig.from_benchmark(code_package)
        function_cfg.docker_image = image_uri
        res = OpenFaaSFunction(func_name, code_package.benchmark, code_package.hash, function_cfg)

        # Add triggers -- invocation still goes through the load-balanced
        # gateway_url, which is exactly what it's designed and proven for.
        http_url = f"{self.config.gateway_url.rstrip('/')}/function/{func_name}"
        self._wait_until_ready(func_name, http_url)
        http_trigger = HTTPTrigger(func_name, http_url)
        http_trigger.logging_handlers = self.logging_handlers
        res.add_trigger(http_trigger)

        cli_trigger = CLITrigger(func_name, self.get_faas_cli_cmd(), self.config.gateway_url)
        cli_trigger.logging_handlers = self.logging_handlers
        res.add_trigger(cli_trigger)

        return res

    def _wait_until_ready(self, func_name: str, invoke_url: str, timeout: float = 60.0, interval: float = 1.0):
        """
        faas-cli deploy returns as soon as the underlying Kubernetes Deployment
        and Service are created -- not once the pod is scheduled, healthy, and
        the gateway's own function list has actually picked it up. Firing an
        invocation immediately after deploy can race this and hit a gateway-
        level 404 ("error finding function ...") even though the deploy itself
        reported success. Poll the real invocation URL and specifically watch
        for that exact 404 signature clearing -- this is method-agnostic (works
        regardless of what the function's own handler expects) and needs no
        gateway auth credentials, unlike polling /system/function/<name>.
        """
        import requests
        import time

        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                resp = requests.get(invoke_url, timeout=5)
                if resp.status_code == 404 and "error finding function" in resp.text:
                    time.sleep(interval)
                    continue
                # Any other response (including errors from the function's own
                # handler, e.g. 405 for a POST-only function) means the gateway
                # now knows about it -- it's ready to actually be invoked.
                return
            except requests.RequestException:
                time.sleep(interval)
        self.logging.warning(
            f"Function {func_name} did not report ready within {timeout}s; "
            "proceeding anyway, the first invocation may fail."
        )

    def update_function(self, function: Function, code_package: Benchmark, system_variant, container_uri: str):
        self.logging.info(f"Update an existing OpenFaaS function {function.name}.")
        function = cast(OpenFaaSFunction, function)
        image_uri = container_uri

        storage_env_args = self.storage_arguments(code_package)
        self._deploy_to_all_clusters(function.name, image_uri, storage_env_args)
        function.config.docker_image = image_uri

        invoke_url = f"{self.config.gateway_url.rstrip('/')}/function/{function.name}"
        self._wait_until_ready(function.name, invoke_url)

    def update_function_configuration(self, function: Function, code_package: Benchmark):
        self.logging.info(f"Update configuration of an existing OpenFaaS function {function.name}.")
        # Re-deploy with new envs
        try:
            self.update_function(function, code_package, code_package.system_variant, function.config.docker_image)
        except Exception as e:
            raise RuntimeError(e)

    def is_configuration_changed(self, cached_function: Function, benchmark: Benchmark) -> bool:
        changed = super().is_configuration_changed(cached_function, benchmark)

        # Safely get storage if available
        storage = None
        if self.system_resources is not None:
            try:
                storage = cast(Minio, self.system_resources.get_storage())
            except RuntimeError:
                storage = None

        function = cast(OpenFaaSFunction, cached_function)
        if storage and function.config.object_storage != storage.config:
            self.logging.info("Updating function configuration due to changed storage configuration.")
            changed = True
            function.config.object_storage = storage.config

        # Safely get NoSQL storage if available
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
        resource_id = resources.resources_id if resources else self.config.docker_registry or "openfaas"
        # Create a hash-based name to satisfy Kubernetes naming constraints (63 char max, alphanumeric + hyphen)
        # Format: sebs-<hash> where hash is derived from the full configuration
        full_name = f"{resource_id}-{code_package.benchmark}-{code_package.language_name}-{code_package.language_version}"
        name_hash = hashlib.md5(full_name.encode()).hexdigest()[:8]
        return f"sebs-{name_hash}"

    def enforce_cold_start(self, functions: List[Function], code_package: Benchmark):
        raise NotImplementedError()

    def download_metrics(self, function_name: str, start_time: int, end_time: int, requests: Dict[str, ExecutionResult], metrics: dict):
        pass

    def create_trigger(self, function: Function, trigger_type: Trigger.TriggerType) -> Trigger:
        if trigger_type == Trigger.TriggerType.LIBRARY:
            trig = function.triggers(Trigger.TriggerType.LIBRARY)[0]
            return trig
        elif trigger_type == Trigger.TriggerType.HTTP:
            # Construct URL from gateway
            url = f"{self.config.gateway_url.rstrip('/')}/function/{function.name}"
            trigger = HTTPTrigger(function.name, url)
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
                trigger.faas_cmd = self.get_faas_cli_cmd()
        for trigger in function.triggers(Trigger.TriggerType.HTTP):
            trigger.logging_handlers = self.logging_handlers

    def disable_rich_output(self):
        self.container_client.disable_rich_output = True