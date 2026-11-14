"""Read-only access to Prometheus, Alertmanager, Loki and the Kubernetes API (GETs only; RBAC allows nothing else)."""
from __future__ import annotations

import os
import time
from typing import Any, Protocol

import httpx2 as httpx

from platform_tools.config import Config


class SourceError(Exception):
    """A data source failed or rejected the request; the message is safe to show the model."""


class Sources(Protocol):
    def prom_query(self, query: str) -> list[dict[str, Any]]: ...
    def prom_query_range(self, query: str, start: float, end: float, step: int) -> list[dict[str, Any]]: ...
    def alerts(self) -> list[dict[str, Any]]: ...
    def loki_query(self, query: str, start: float, end: float, limit: int) -> list[dict[str, Any]]: ...
    def pods(self, namespace: str, label_selector: str = "") -> list[dict[str, Any]]: ...
    def events(self, namespace: str) -> list[dict[str, Any]]: ...
    def pod_log(self, namespace: str, pod: str, container: str | None, tail: int, previous: bool) -> str: ...
    def nodes(self) -> list[dict[str, Any]]: ...
    def custom(self, group: str, version: str, namespace: str, plural: str, name: str) -> dict[str, Any]: ...
    def custom_list(self, group: str, version: str, namespace: str, plural: str) -> list[dict[str, Any]]: ...


class LiveSources:
    """Sources backed by the in-cluster services and the Kubernetes API."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._http = httpx.Client(timeout=config.http_timeout_seconds)
        from kubernetes import client, config as kube_config

        try:
            kube_config.load_incluster_config()
        except kube_config.ConfigException:     # local development against kind
            kube_config.load_kube_config(context=os.environ.get("KUBE_CONTEXT") or None)
        self._core = client.CoreV1Api()
        self._custom = client.CustomObjectsApi()
        self._api_exception = client.ApiException

    def _get(self, url: str, params: dict[str, Any]) -> Any:
        try:
            response = self._http.get(url, params=params)
        except httpx.HTTPError as e:
            raise SourceError(f"{url.split('/api')[0]} unreachable: {type(e).__name__}") from e
        if response.status_code >= 400:
            raise SourceError(f"HTTP {response.status_code}: {response.text[:300]}")
        return response.json()

    def prom_query(self, query: str) -> list[dict[str, Any]]:
        body = self._get(f"{self._config.prometheus_url}/api/v1/query", {"query": query})
        return body["data"]["result"]

    def prom_query_range(self, query: str, start: float, end: float, step: int) -> list[dict[str, Any]]:
        body = self._get(f"{self._config.prometheus_url}/api/v1/query_range",
                         {"query": query, "start": start, "end": end, "step": step})
        return body["data"]["result"]

    def alerts(self) -> list[dict[str, Any]]:
        return self._get(f"{self._config.alertmanager_url}/api/v2/alerts",
                         {"active": "true", "silenced": "false", "inhibited": "false"})

    def loki_query(self, query: str, start: float, end: float, limit: int) -> list[dict[str, Any]]:
        body = self._get(f"{self._config.loki_url}/loki/api/v1/query_range",
                         {"query": query, "start": int(start * 1e9), "end": int(end * 1e9),
                          "limit": limit, "direction": "backward"})
        return body["data"]["result"]

    def _kube(self, call, *args, **kwargs):
        try:
            return call(*args, **kwargs)
        except self._api_exception as e:
            raise SourceError(f"Kubernetes API {e.status}: {e.reason}") from e

    def pods(self, namespace: str, label_selector: str = "") -> list[dict[str, Any]]:
        result = self._kube(self._core.list_namespaced_pod, namespace, label_selector=label_selector)
        return [p.to_dict() for p in result.items]

    def events(self, namespace: str) -> list[dict[str, Any]]:
        result = self._kube(self._core.list_namespaced_event, namespace)
        return [e.to_dict() for e in result.items]

    def pod_log(self, namespace: str, pod: str, container: str | None, tail: int, previous: bool) -> str:
        kwargs: dict[str, Any] = {"tail_lines": tail, "previous": previous}
        if container:
            kwargs["container"] = container
        return self._kube(self._core.read_namespaced_pod_log, pod, namespace, **kwargs)

    def nodes(self) -> list[dict[str, Any]]:
        return [n.to_dict() for n in self._kube(self._core.list_node).items]

    def custom(self, group: str, version: str, namespace: str, plural: str, name: str) -> dict[str, Any]:
        return self._kube(self._custom.get_namespaced_custom_object, group, version, namespace, plural, name)

    def custom_list(self, group: str, version: str, namespace: str, plural: str) -> list[dict[str, Any]]:
        return self._kube(self._custom.list_namespaced_custom_object, group, version, namespace, plural)["items"]


def now() -> float:
    return time.time()
