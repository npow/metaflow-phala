"""Metaflow @phala step decorator.

Runs a Metaflow step inside a Phala Cloud CVM (Confidential Virtual Machine).

Usage::

    @phala(image="python:3.11-slim", cpu=2, memory=4096)
    @step
    def my_step(self):
        ...

Requires:
- PHALA_API_KEY environment variable (or METAFLOW_PHALA_API_KEY)
- A remote datastore (s3, azure, gs) — same requirement as @batch / @kubernetes
"""

from __future__ import annotations

import os
import sys
from typing import Any, ClassVar

from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException

_DEFAULT_IMAGE = os.environ.get("METAFLOW_PHALA_IMAGE", "python:3.11-slim")
_DEFAULT_CPU = int(os.environ.get("METAFLOW_PHALA_CPU", "2"))
_DEFAULT_MEMORY = int(os.environ.get("METAFLOW_PHALA_MEMORY", "2048"))
_DEFAULT_DISK = int(os.environ.get("METAFLOW_PHALA_DISK", "20"))
_DEFAULT_TIMEOUT = int(os.environ.get("METAFLOW_PHALA_TIMEOUT", "3600"))

# env var names that hold the API key (checked in priority order)
_API_KEY_ENV_VARS = ("PHALA_API_KEY", "METAFLOW_PHALA_API_KEY")


def _get_api_key() -> str | None:
    for var in _API_KEY_ENV_VARS:
        val = os.environ.get(var)
        if val:
            return val
    return None


class PhalaException(MetaflowException):
    headline = "Phala error"


class PhalaDecorator(StepDecorator):
    """Run a Metaflow step inside a Phala Cloud TEE CVM.

    Parameters
    ----------
    image : str
        Docker image to run inside the CVM.
        Default: python:3.11-slim (or METAFLOW_PHALA_IMAGE env var).
    cpu : int
        Number of vCPUs to allocate to the CVM.
    memory : int
        Memory in MB to allocate to the CVM.
    disk : int
        Disk size in GB.
    timeout : int
        Maximum seconds to wait for the step to complete.
    env : dict
        Additional environment variables to inject into the container.
    """

    name = "phala"

    defaults: ClassVar[dict[str, Any]] = {
        "image": None,  # resolved at step_init from env or default
        "cpu": None,
        "memory": None,
        "disk": None,
        "timeout": None,
        "env": {},
    }

    supports_conda_environment = True

    # Class-level package state — shared across all decorator instances so the
    # code package is uploaded exactly once per flow run (same pattern as @batch).
    package_metadata: ClassVar[str | None] = None
    package_url: ClassVar[str | None] = None
    package_sha: ClassVar[str | None] = None

    def step_init(
        self,
        flow: Any,
        graph: Any,
        step_name: str,
        decorators: Any,
        environment: Any,
        flow_datastore: Any,
        logger: Any,
    ) -> None:
        self._step_name = step_name
        self.flow_datastore = flow_datastore
        self.environment = environment
        self.logger = logger
        self.attributes.setdefault("env", {})

        # Resolve resource defaults
        if self.attributes["image"] is None:
            self.attributes["image"] = _DEFAULT_IMAGE
        if self.attributes["cpu"] is None:
            self.attributes["cpu"] = _DEFAULT_CPU
        if self.attributes["memory"] is None:
            self.attributes["memory"] = _DEFAULT_MEMORY
        if self.attributes["disk"] is None:
            self.attributes["disk"] = _DEFAULT_DISK
        if self.attributes["timeout"] is None:
            self.attributes["timeout"] = _DEFAULT_TIMEOUT

        # Require a remote datastore (artifacts must persist beyond the CVM).
        if flow_datastore.TYPE == "local":
            raise PhalaException(
                "@phala requires a remote datastore (s3, azure, gs). "
                "Set METAFLOW_DEFAULT_DATASTORE=s3 and configure credentials.\n"
                "See https://docs.metaflow.org/scaling/remote-tasks/introduction"
            )

        # Require API key at decoration time to fail early.
        if not _get_api_key():
            raise PhalaException(
                "@phala requires a Phala Cloud API key. "
                f"Set one of: {', '.join(_API_KEY_ENV_VARS)}"
            )

    def runtime_init(self, flow: Any, graph: Any, package: Any, run_id: str) -> None:
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

    def runtime_task_created(
        self,
        task_datastore: Any,
        task_id: str,
        split_index: Any,
        input_paths: Any,
        is_cloned: bool,
        ubf_context: Any,
    ) -> None:
        if not is_cloned:
            self._save_package_once(self.flow_datastore, self.package)

    def runtime_step_cli(
        self,
        cli_args: Any,
        retry_count: int,
        max_user_code_retries: int,
        ubf_context: Any,
    ) -> None:
        """Redirect execution to ``phala step`` CLI command."""
        if os.environ.get("METAFLOW_PHALA_WORKLOAD"):
            return

        if retry_count <= max_user_code_retries:
            cli_args.commands = ["phala", "step"]
            cli_args.command_args.append(self.package_metadata)
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)

            _skip_keys = {"env"}
            cli_args.command_options.update(
                {k: v for k, v in self.attributes.items() if k not in _skip_keys}
            )

            # Pass user env vars as repeated --env-var KEY=VALUE
            user_env = dict(self.attributes.get("env") or {})
            if user_env:
                cli_args.command_options["env-var"] = [
                    f"{k}={v}" for k, v in user_env.items()
                ]

            cli_args.entrypoint[0] = sys.executable

    def task_pre_step(
        self,
        step_name: str,
        task_datastore: Any,
        metadata: Any,
        run_id: str,
        task_id: str,
        flow: Any,
        graph: Any,
        retry_count: int,
        max_user_code_retries: int,
        ubf_context: Any,
        inputs: Any,
    ) -> None:
        """Emit execution metadata when running inside the CVM."""
        self.metadata = metadata
        self.task_datastore = task_datastore

        if os.environ.get("METAFLOW_PHALA_WORKLOAD"):
            from metaflow.metadata_provider import MetaDatum

            entries = [
                MetaDatum(
                    field="phala-cvm-id",
                    value=os.environ.get("METAFLOW_PHALA_CVM_ID", ""),
                    type="phala-cvm-id",
                    tags=[f"attempt_id:{retry_count}"],
                ),
            ]
            metadata.register_metadata(run_id, step_name, task_id, entries)

    def task_finished(
        self,
        step_name: str,
        flow: Any,
        graph: Any,
        is_task_ok: bool,
        retry_count: int,
        max_retries: int,
    ) -> None:
        if (
            os.environ.get("METAFLOW_PHALA_WORKLOAD")
            and hasattr(self, "metadata")
            and self.metadata.TYPE == "local"
        ):
            from metaflow.metadata_provider.util import sync_local_metadata_to_datastore
            from metaflow.metaflow_config import DATASTORE_LOCAL_DIR

            sync_local_metadata_to_datastore(DATASTORE_LOCAL_DIR, self.task_datastore)

    @classmethod
    def _save_package_once(cls, flow_datastore: Any, package: Any) -> None:
        """Upload code package to remote datastore (once per flow run)."""
        if PhalaDecorator.package_url is None:
            url, sha = flow_datastore.save_data([package.blob], len_hint=1)[0]
            PhalaDecorator.package_url = url
            PhalaDecorator.package_sha = sha
            PhalaDecorator.package_metadata = package.package_metadata
