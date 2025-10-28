"""Client abstractions for the IEC 60870-5-104 connector."""

from __future__ import annotations

from abc import ABC, abstractmethod
from importlib import import_module
from time import time
from typing import Any, Callable, Dict, Iterable, Optional


UpdateCallback = Callable[[Dict[str, Any]], None]


class IEC104ClientBase(ABC):
    """Base interface for IEC 60870-5-104 client implementations."""

    def __init__(self, config: Dict[str, Any], logger):
        self._config = config
        self._logger = logger
        self._listener: Optional[UpdateCallback] = None

    def set_listener(self, listener: UpdateCallback) -> None:
        self._listener = listener

    @abstractmethod
    def connect(self) -> None:
        """Establish connection with the IEC-104 server."""

    @abstractmethod
    def close(self) -> None:
        """Terminate connection with the IEC-104 server."""

    @abstractmethod
    def interrogate(self, device_config: Dict[str, Any], points: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        """Trigger interrogation for the provided points."""

    @abstractmethod
    def send_command(self, request: Dict[str, Any]) -> Any:
        """Send command (control operation) to the IEC-104 server."""

    def _emit_update(self, payload: Dict[str, Any]) -> None:
        if self._listener:
            try:
                self._listener(payload)
            except Exception:  # pragma: no cover - defensive logging
                self._logger.exception("Failed to handle IEC-104 update callback")


class DummyIEC104Client(IEC104ClientBase):
    """Fallback client used when a real IEC-104 implementation is unavailable."""

    def connect(self) -> None:  # pragma: no cover - logging only
        self._logger.warning(
            "Dummy IEC-104 client is active. No real IEC-104 traffic will be exchanged."
        )

    def close(self) -> None:  # pragma: no cover - nothing to clean up
        self._logger.debug("Dummy IEC-104 client closed")

    def interrogate(
        self, device_config: Dict[str, Any], points: Iterable[Dict[str, Any]]
    ) -> Iterable[Dict[str, Any]]:
        results = []
        timestamp = int(time() * 1000)
        for point in points:
            if not point.get("simulate", True):
                continue
            value = point.get("defaultValue")
            io_address = point.get("ioAddress")
            if value is None or io_address is None:
                continue
            update = {
                "common_address": point.get("commonAddress", device_config.get("commonAddress")),
                "io_address": io_address,
                "value": value,
                "quality": point.get("quality", 0),
                "ts": timestamp,
            }
            results.append(update)
            self._emit_update(update)
        return results

    def send_command(self, request: Dict[str, Any]) -> Any:  # pragma: no cover - logging only
        self._logger.warning("Dummy IEC-104 client cannot execute command: %%s", request)
        return {"success": False, "error": "Dummy IEC-104 client"}


class IEC104ClientFactory:
    """Factory helper that instantiates IEC-104 client implementations."""

    DEFAULT_CLASS_PATH = "thingsboard_gateway.connectors.iec104.client.DummyIEC104Client"

    @classmethod
    def create(cls, config: Dict[str, Any], logger) -> IEC104ClientBase:
        class_path = config.get("class", cls.DEFAULT_CLASS_PATH)
        module_name, _, class_name = class_path.rpartition(".")
        if not module_name:
            raise ImportError(f"Invalid IEC-104 client class path: {class_path}")

        module = import_module(module_name)
        client_class = getattr(module, class_name)
        if not issubclass(client_class, IEC104ClientBase):
            raise TypeError(f"IEC-104 client {class_path} must inherit from IEC104ClientBase")

        return client_class(config, logger)
