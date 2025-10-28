"""Default uplink converter for IEC 60870-5-104 connector."""

from __future__ import annotations

from typing import Any, Dict

from thingsboard_gateway.connectors.converter import Converter
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService


class IEC104UplinkConverter(Converter):
    """Converts IEC-104 updates to ThingsBoard telemetry/attributes."""

    def __init__(self, config: Dict[str, Any], logger):
        self._config = config
        self._logger = logger

    def convert(self, config: Dict[str, Any], data: Dict[str, Any]) -> ConvertedData:
        device_config = config["device"]
        point_config = config["point"]

        device_name = device_config.get("deviceName")
        device_type = device_config.get("deviceType", "default")
        metadata = device_config.get("metadata")

        converted = ConvertedData(device_name=device_name, device_type=device_type, metadata=metadata)

        key = point_config.get("key", point_config.get("name") or point_config.get("ioAddress"))
        if key is None:
            self._logger.error("IEC-104 mapping for device %s is missing key configuration", device_name)
            return converted

        value = data.get("value")
        if "multiplier" in point_config:
            try:
                value = value * point_config["multiplier"]
            except Exception:  # pragma: no cover - logging only
                self._logger.exception("Failed to apply multiplier for %s", key)

        datapoint_type = point_config.get("type", "telemetry").lower()

        if datapoint_type == "attribute":
            converted.add_to_attributes(key, value)
            StatisticsService.count_connector_message(
                self._logger.name, 'convertersAttrProduced', count=1
            )
        else:
            telemetry_entry = TelemetryEntry({key: value}, data.get("ts"))
            converted.add_to_telemetry(telemetry_entry)
            StatisticsService.count_connector_message(
                self._logger.name, 'convertersTsProduced', count=len(telemetry_entry.values)
            )

        return converted
