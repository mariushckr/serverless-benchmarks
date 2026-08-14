import os
import shutil
import platform
import docker
from typing import Tuple

from sebs.faas.container import DockerContainer
from sebs.config import SeBSConfig
from sebs.utils import get_resource_path
from .config import OpenFaaSConfig


class OpenFaaSContainer(DockerContainer):
    @staticmethod
    def name() -> str:
        return "openfaas"

    @staticmethod
    def typename() -> str:
        return "OpenFaaS.Container"

    def __init__(self, system_config: SeBSConfig, config: OpenFaaSConfig, docker_client: docker.client, experimental_manifest: bool):
        super().__init__(system_config, docker_client, experimental_manifest)
        self.config = config

    def registry_name(self, benchmark: str, language_name: str, language_version: str, architecture: str) -> Tuple[str, str, str, str]:
        registry_name = self.config.docker_registry

        repository_name = self.system_config.docker_repository()
        image_tag = self.system_config.benchmark_image_tag(
            self.name(), benchmark, language_name, language_version, architecture
        )
        if registry_name is not None and registry_name != "":
            repository_name = f"{registry_name}/{repository_name}"
        else:
            registry_name = "Docker Hub"
        image_uri = f"{repository_name}:{image_tag}"

        return registry_name, repository_name, image_tag, image_uri

    def build_base_image(
        self,
        directory: str,
        language_name: str,
        language_version: str,
        architecture: str,
        benchmark: str,
        is_cached: bool,
        builder_image: str = "",
    ) -> Tuple[bool, str, float]:
        """
        Override of parent build_base_image to include __main__.py wrapper.
        OpenFaaS requires __main__.py to be in the build context, unlike OpenWhisk.

        NOTE: the 7th positional arg (builder_image) is passed by Benchmark.build()
        for every container-deployment platform's calling convention, but OpenFaaS
        builds the function image directly via the Docker SDK (using Dockerfile.function
        + BASE_IMAGE buildarg) rather than running a separate pre-built builder
        container -- so it's accepted here but not used.
        """

        registry_name, repository_name, image_tag, image_uri = self.registry_name(
            benchmark, language_name, language_version, architecture
        )

        # cached package, rebuild not enforced -> check for new one
        if is_cached:
            if self.find_image(repository_name, image_tag):
                self.logging.info(
                    f"Skipping building Docker image for {benchmark}, using "
                    f"Docker image {image_uri} from registry: {registry_name}."
                )
                return False, image_uri, self._directory_size_mb(directory)
            else:
                self.logging.info(
                    f"Image {image_uri} doesn't exist in the registry, "
                    f"building the image for {benchmark}."
                )

        build_dir = os.path.join(directory, "build")
        os.makedirs(build_dir, exist_ok=True)

        dockerfile_path = get_resource_path("dockerfiles")
        shutil.copy(
            os.path.join(dockerfile_path, self.name(), language_name, "Dockerfile.function"),
            os.path.join(build_dir, "Dockerfile"),
        )
        
        # For OpenFaaS, copy __main__.py to build directory
        for fn in os.listdir(directory):
            file = os.path.join(directory, fn)
            if fn == "index.js":
                # Skip index.js for Python deployments
                if language_name == "python":
                    continue
            if os.path.isdir(file):
                # Copy directories
                if os.path.exists(os.path.join(build_dir, fn)):
                    shutil.rmtree(os.path.join(build_dir, fn))
                shutil.copytree(file, os.path.join(build_dir, fn))
            else:
                # Copy files (including __main__.py for OpenFaaS)
                shutil.copy2(file, os.path.join(build_dir, fn))

        # Ensure requirements.txt exists for Docker build
        # If only version-specific requirements.txt.X exists, copy it to requirements.txt
        if language_name == "python":
            req_file = os.path.join(build_dir, "requirements.txt")
            if not os.path.exists(req_file):
                version_req = os.path.join(build_dir, f"requirements.txt.{language_version}")
                if os.path.exists(version_req):
                    shutil.copy2(version_req, req_file)

        with open(os.path.join(build_dir, ".dockerignore"), "w") as f:
            f.write("Dockerfile")

        builder_base_image = self.system_config.benchmark_base_images(
            self.name(), language_name, architecture
        )[language_version]
        self.logging.info(f"Build the benchmark base image {repository_name}:{image_tag}.")

        isa = platform.processor()
        if (isa == "x86_64" and architecture != "x64") or (
            isa == "arm64" and architecture != "arm64"
        ):
            self.logging.warning(
                f"Building image for architecture: {architecture} on CPU architecture: {isa}. "
                "This step requires configured emulation."
            )

        buildargs = {
            "VERSION": language_version,
            "BASE_IMAGE": builder_base_image,
            "TARGET_ARCHITECTURE": architecture,
        }
        image, _ = self.docker_client.images.build(
            tag=image_uri, path=build_dir, buildargs=buildargs
        )

        self.logging.info(
            f"Push the benchmark base image {repository_name}:{image_tag} "
            f"to registry: {registry_name}."
        )

        self.push_image(image_uri, image_tag)

        return True, image_uri, self._directory_size_mb(directory)

    @staticmethod
    def _directory_size_mb(directory: str) -> float:
        """Total size in MB of all files under directory, matching the units
        Benchmark._code_size is expected to hold (see Benchmark.directory_size)."""
        total_bytes = 0
        for root, _, files in os.walk(directory):
            for f in files:
                try:
                    total_bytes += os.path.getsize(os.path.join(root, f))
                except OSError:
                    continue
        return total_bytes / 1024.0 / 1024.0