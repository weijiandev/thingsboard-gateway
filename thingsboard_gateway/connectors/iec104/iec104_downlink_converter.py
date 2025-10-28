"""Default downlink converter for IEC 60870-5-104 connector."""

from __future__ import annotations

from typing import Any, Dict

from thingsboard_gateway.connectors.converter import Converter


class IEC104DownlinkConverter(Converter):
    """Converts ThingsBoard RPC/attribute updates into IEC-104 commands."""

    def __init__(self, config: Dict[str, Any], logger):
        self._config = config
        self._logger = logger

    def convert(self, config: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
        point_config = config.get("point", config)
        params = data.get("data", {}).get("params", {}) if isinstance(data.get("data"), dict) else {}

        request: Dict[str, Any] = {
            "common_address": point_config.get("commonAddress", config.get("commonAddress")),
            "io_address": point_config.get("ioAddress"),
            "value": params.get("value", params or data.get("value")),
            "command": point_config.get("command", "single"),
            "select_before_operate": point_config.get("selectBeforeOperate", False),
        }

        if "quality" in point_config:
            request["quality"] = point_config["quality"]

        additional = point_config.get("additionalParameters", {})
        if isinstance(additional, dict):
            request.update(additional)

        return request
