"""Phala Cloud REST API client.

Wraps the cloud-api.phala.network v1 API for CVM lifecycle management.
Authentication uses X-API-Key header.
"""

from __future__ import annotations

import time
from typing import Any

import requests

API_BASE = "https://cloud-api.phala.network/api/v1"
_POLL_INTERVAL = 10  # seconds between status polls


class PhalaException(Exception):
    pass


class PhalaClient:
    def __init__(self, api_key: str) -> None:
        self._session = requests.Session()
        self._session.headers.update(
            {
                "X-API-Key": api_key,
                "Content-Type": "application/json",
            }
        )

    # ------------------------------------------------------------------
    # Provisioning
    # ------------------------------------------------------------------

    def provision(
        self,
        name: str,
        compose_file: dict[str, Any],
        vcpu: int = 2,
        memory: int = 2048,
        disk_size: int = 20,
    ) -> dict[str, Any]:
        """Provision a CVM app (step 1 of 2).

        Returns a dict containing ``compose_hash`` and ``app_id`` needed
        to create the actual CVM.
        """
        resp = self._session.post(
            f"{API_BASE}/cvms/provision",
            json={
                "name": name,
                "compose_file": compose_file,
                "vcpu": vcpu,
                "memory": memory,
                "disk_size": disk_size,
            },
        )
        _raise_for_status(resp)
        return resp.json()

    def create_cvm(self, app_id: str, compose_hash: str) -> dict[str, Any]:
        """Create and start a CVM (step 2 of 2).

        Returns the VM object with ``id``, ``status``, etc.
        """
        resp = self._session.post(
            f"{API_BASE}/cvms",
            json={"app_id": app_id, "compose_hash": compose_hash},
        )
        _raise_for_status(resp)
        return resp.json()

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def get_cvm(self, cvm_id: int) -> dict[str, Any]:
        """Return current CVM details including ``status`` field."""
        resp = self._session.get(f"{API_BASE}/cvms/{cvm_id}")
        _raise_for_status(resp)
        return resp.json()

    def list_cvms(self) -> list[dict[str, Any]]:
        resp = self._session.get(f"{API_BASE}/cvms")
        _raise_for_status(resp)
        return resp.json()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def delete_cvm(self, cvm_id: int) -> None:
        """Delete a CVM. Silently ignores 404 (already gone)."""
        resp = self._session.delete(f"{API_BASE}/cvms/{cvm_id}")
        if resp.status_code not in (200, 204, 404):
            _raise_for_status(resp)

    # ------------------------------------------------------------------
    # Polling helpers
    # ------------------------------------------------------------------

    def wait_for_running(self, cvm_id: int, timeout: int = 300) -> None:
        """Block until the CVM reaches ``running`` status.

        Raises PhalaException on error or timeout.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            cvm = self.get_cvm(cvm_id)
            status = cvm.get("status", "")
            if status == "running":
                return
            if _is_terminal_failure(status):
                raise PhalaException(
                    f"CVM {cvm_id} entered terminal state {status!r} before running. "
                    f"CVM details: {cvm}"
                )
            time.sleep(_POLL_INTERVAL)
        raise PhalaException(
            f"Timeout waiting for CVM {cvm_id} to start (waited {timeout}s). "
            "Check the Phala Cloud dashboard for details."
        )

    def is_stopped(self, cvm_id: int) -> bool:
        """Return True if the CVM has stopped (container exited)."""
        try:
            cvm = self.get_cvm(cvm_id)
        except Exception:
            return False
        return _is_terminal_state(cvm.get("status", ""))


def _raise_for_status(resp: requests.Response) -> None:
    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:
        try:
            detail = resp.json()
        except Exception:
            detail = resp.text
        raise PhalaException(
            f"Phala API error {resp.status_code}: {detail}"
        ) from exc


def _is_terminal_failure(status: str) -> bool:
    return status.lower() in ("error", "failed", "terminated")


def _is_terminal_state(status: str) -> bool:
    return status.lower() in ("stopped", "error", "failed", "terminated", "exited")
