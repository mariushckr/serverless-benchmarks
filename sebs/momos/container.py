import os
import shutil
import platform
import docker
import subprocess
from typing import Tuple

from sebs.faas.container import DockerContainer
from sebs.config import SeBSConfig
from sebs.utils import get_resource_path
from .config import MomosConfig


class MomosContainer(DockerContainer):
    @staticmethod
    def name() -> str:
        return "momos"

    @staticmethod
    def typename() -> str:
        return "Momos.Container"

    def __init__(self, system_config: SeBSConfig, config: MomosConfig, docker_client: docker.client, experimental_manifest: bool):
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

        registry_name, repository_name, image_tag, image_uri = self.registry_name(
            benchmark, language_name, language_version, architecture
        )

        # cached package, rebuild not enforced → check for existing image
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

        for fn in os.listdir(directory):
            file = os.path.join(directory, fn)
            if fn == "index.js" and language_name == "python":
                continue
            if os.path.isdir(file):
                if os.path.exists(os.path.join(build_dir, fn)):
                    shutil.rmtree(os.path.join(build_dir, fn))
                shutil.copytree(file, os.path.join(build_dir, fn))
            else:
                shutil.copy2(file, os.path.join(build_dir, fn))

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

        # MOMOS_IMAGE_NAME must match the registry-STRIPPED name -- this is
        # what the dispatch side actually uses as the Redis queue key
        # (confirmed via the real Kafka message: imageId is stripped, not
        # registry-qualified). Using the full image_uri here was baking a
        # queue name into the container that nothing ever pushes work to,
        # so the container ran forever without ever picking anything up.
        registered_name = self._image_stem(image_uri)

        buildargs = {
            "VERSION": language_version,
            "BASE_IMAGE": builder_base_image,
            "TARGET_ARCHITECTURE": architecture,
            "MOMOS_IMAGE_NAME": registered_name,
        }

        # Tag with the full registry-qualified image_uri (not the bare
        # image_tag) -- the cached-image-check above already correctly uses
        # image_uri; building without the registry prefix meant the image
        # was never actually associated with the target registry namespace
        # at the Docker layer, inconsistent with that cached path.
        cmd = ["docker", "build", "-t", image_uri, build_dir]
        for key, value in buildargs.items():
            cmd.extend(["--build-arg", f"{key}={value}"])

        subprocess.run(cmd, check=True)

        self.logging.info(
            f"Push the benchmark base image {repository_name}:{image_tag} "
            f"to registry: {registry_name}."
        )

        # Use momos deploy to register + push the image
        self._momos_deploy(image_uri)

        return True, image_uri, self._directory_size_mb(directory)

    @staticmethod
    def _image_stem(image: str) -> str:
        """Mirrors momos-cli's own imageStem() exactly -- strips a leading
        registry host (detected by containing '.' or ':' before the first
        '/') from an image reference. Kept local rather than importing
        from momos.py to avoid coupling these two classes over a 3-line
        utility; must stay in sync with Momos.image_stem() in momos.py."""
        parts = image.split("/", 1)
        if len(parts) == 2 and any(c in parts[0] for c in ".:"):
            return parts[1]
        return image

    @staticmethod
    def _directory_size_mb(directory: str) -> float:
        total_bytes = 0
        for root, _, files in os.walk(directory):
            for f in files:
                try:
                    total_bytes += os.path.getsize(os.path.join(root, f))
                except OSError:
                    continue
        return total_bytes / 1024.0 / 1024.0

    def _momos_deploy(self, image_uri: str):
        """
        Register the image with the Momos API and push it to the assigned
        registry using momos-cli.

        Equivalent to: momos deploy --image <image_uri> --gateway <url>
        """
        cmd = [
            self.config.momos_cli,
            "--gateway", self.config.gateway_url,
            "deploy",
            "--image", image_uri,
        ]
        self.logging.info(f"Deploying image {image_uri} via momos-cli.")
        try:
            subprocess.run(cmd, check=True)
        except FileNotFoundError:
            raise RuntimeError(
                f"momos-cli binary not found at '{self.config.momos_cli}'. "
                "Make sure momos is installed and on your PATH."
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"momos deploy failed for {image_uri}: {e}")