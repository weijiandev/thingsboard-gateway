"""IEC 60870-5-104 connector implementation."""

from __future__ import annotations

from queue import Empty, Queue
from random import choice
from re import fullmatch
from string import ascii_lowercase
from threading import Event, Thread
from time import sleep
from typing import Any, Dict, List, Optional, Tuple

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.connectors.iec104.client import DummyIEC104Client, IEC104ClientFactory
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_logger import init_logger

DEFAULT_UPLINK_CONVERTER = "IEC104UplinkConverter"
DEFAULT_DOWNLINK_CONVERTER = "IEC104DownlinkConverter"


class IEC104Connector(Connector, Thread):
    """Connector responsible for exchanging data with IEC-104 servers."""

    def __init__(self, gateway, config: Dict[str, Any], connector_type: str):
        super().__init__()
        self._gateway = gateway
        self._connector_type = connector_type
        self._config = config
        self._id = config.get("id")
        self.name = config.get(
            "name", f"IEC104 Connector {''.join(choice(ascii_lowercase) for _ in range(5))}"
        )

        log_level = config.get("logLevel", "INFO")
        remote_logging = config.get("enableRemoteLogging", False)
        self._log = init_logger(
            self._gateway,
            self.name,
            log_level,
            enable_remote_logging=remote_logging,
            is_connector_logger=True,
        )
        self._converter_log = init_logger(
            self._gateway,
            f"{self.name}_converter",
            log_level,
            enable_remote_logging=remote_logging,
            is_converter_logger=True,
            attr_name=self.name,
        )

        self.daemon = True
        self._connected = False
        self._stopped = Event()

        try:
            self._client = IEC104ClientFactory.create(config.get("client", {}), self._log)
        except (ImportError, AttributeError, TypeError, ValueError) as exc:
            self._log.error("Failed to load IEC-104 client implementation: %s", exc)
            self._client = DummyIEC104Client(config.get("client", {}), self._log)
        self._client.set_listener(self._on_point_update)

        self._data_queue: Queue = Queue(-1)
        self._command_queue: Queue = Queue(-1)
        self._processing_thread: Optional[Thread] = None
        self._command_thread: Optional[Thread] = None
        self._polling_threads: List[Thread] = []

        self._points_map: Dict[Tuple[Any, Any], Dict[str, Any]] = {}
        self._attribute_updates: List[Dict[str, Any]] = []
        self._rpc_requests: List[Dict[str, Any]] = []

        self._load_devices(config.get("devices", []))

    # region base connector API
    def open(self) -> None:
        self._stopped.clear()
        self.start()

    def close(self) -> None:
        self._stopped.set()
        self._connected = False

        if self._client:
            try:
                self._client.close()
            except Exception:  # pragma: no cover - defensive logging
                self._log.exception("Failed to close IEC-104 client")

        if self._processing_thread and self._processing_thread.is_alive():
            self._processing_thread.join(timeout=5)
        if self._command_thread and self._command_thread.is_alive():
            self._command_thread.join(timeout=5)

        for thread in self._polling_threads:
            if thread.is_alive():
                thread.join(timeout=5)

        self._log.info("%s has been stopped", self.name)
        self._log.stop()
        self._converter_log.stop()

    def get_id(self):
        return self._id

    def get_name(self):
        return self.name

    def get_type(self):
        return self._connector_type

    def get_config(self):
        return self._config

    def is_connected(self):
        return self._connected

    def is_stopped(self):
        return self._stopped.is_set()

    # endregion

    def run(self) -> None:
        self._log.info("Starting IEC-104 connector")
        try:
            self._client.connect()
            self._connected = True
        except Exception as e:  # pragma: no cover - connection errors are logged only
            self._log.exception("Failed to establish IEC-104 connection: %s", e)
            self._connected = False

        self._processing_thread = Thread(target=self._process_data, daemon=True, name="IEC104 Data Processor")
        self._processing_thread.start()

        self._command_thread = Thread(target=self._process_commands, daemon=True, name="IEC104 Command Processor")
        self._command_thread.start()

        for device in self._config.get("devices", []):
            poll_period = device.get("pollPeriod", self._config.get("defaultPollPeriod"))
            if poll_period:
                thread = Thread(
                    target=self._poll_device,
                    args=(device, poll_period),
                    daemon=True,
                    name=f"IEC104 Polling {device.get('deviceName', 'Unknown')}",
                )
                thread.start()
                self._polling_threads.append(thread)

        while not self._stopped.is_set():
            sleep(1)

    # region ThingsBoard handlers
    def on_attributes_update(self, content):
        try:
            for request in self._attribute_updates:
                if not fullmatch(request["deviceNameFilter"], content["device"]):
                    continue

                attribute_keys = list(content.get("data", {}).keys())
                if not attribute_keys:
                    continue

                if not fullmatch(request["attributeFilter"], attribute_keys[0]):
                    continue

                converter = request.get("converter")
                if not converter:
                    continue

                payload = converter.convert(request, content)
                if payload:
                    self._command_queue.put({"request": payload})
        except Exception:  # pragma: no cover - defensive logging
            self._log.exception("Failed to process attribute update for IEC-104 connector")

    def server_side_rpc_handler(self, content):
        try:
            for rpc_request in self._rpc_requests:
                if not fullmatch(rpc_request["deviceNameFilter"], content["device"]):
                    continue
                if not fullmatch(rpc_request["methodFilter"], content["data"]["method"]):
                    continue

                converter = rpc_request.get("converter")
                if not converter:
                    continue

                payload = converter.convert(rpc_request, content)
                if payload is None:
                    continue

                self._command_queue.put({"request": payload, "originator": content})
                return
        except Exception:  # pragma: no cover - defensive logging
            self._log.exception("Failed to handle RPC request for IEC-104 connector")
    # endregion

    # region internal helpers
    def _load_devices(self, devices: List[Dict[str, Any]]) -> None:
        for device in devices:
            converter_class_name = device.get("converter", DEFAULT_UPLINK_CONVERTER)
            converter_cls = TBModuleLoader.import_module(self._connector_type, converter_class_name)
            if not converter_cls:
                self._log.error("Cannot find uplink converter %s for device %s", converter_class_name, device)
                continue

            converter = converter_cls(device, self._converter_log)

            for point in device.get("points", []):
                common_address = point.get("commonAddress", device.get("commonAddress"))
                io_address = point.get("ioAddress")
                if io_address is None:
                    self._log.error("IEC-104 point in device %s is missing ioAddress", device.get("deviceName"))
                    continue

                key = (common_address, io_address)
                self._points_map[key] = {
                    "device": device,
                    "point": point,
                    "converter": converter,
                }

            for attr in device.get("attributeUpdates", []):
                self._attribute_updates.append(self._prepare_downlink_config(device, attr))

            for rpc in device.get("serverSideRpc", []):
                self._rpc_requests.append(self._prepare_downlink_config(device, rpc))

    def _prepare_downlink_config(self, device: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        prepared = dict(config)
        prepared.setdefault("deviceNameFilter", device.get("deviceName", ".*"))
        prepared.setdefault("methodFilter", config.get("method", ".*"))
        prepared.setdefault("attributeFilter", config.get("attribute", ".*"))
        prepared.setdefault("commonAddress", config.get("commonAddress", device.get("commonAddress")))
        prepared["device"] = device
        prepared["point"] = config

        converter_name = config.get("converter", DEFAULT_DOWNLINK_CONVERTER)
        converter_cls = TBModuleLoader.import_module(self._connector_type, converter_name)
        if not converter_cls:
            self._log.error("Cannot find downlink converter %s for device %s", converter_name, device)
            return prepared

        prepared["converter"] = converter_cls(config, self._converter_log)
        return prepared

    def _poll_device(self, device: Dict[str, Any], poll_period: int) -> None:
        while not self._stopped.wait(poll_period):
            try:
                responses = self._client.interrogate(device, device.get("points", []))
                for response in responses:
                    self._on_point_update(response)
            except Exception:  # pragma: no cover - defensive logging
                self._log.exception("Failed to interrogate IEC-104 device %s", device.get("deviceName"))

    def _on_point_update(self, data: Dict[str, Any]) -> None:
        self._data_queue.put(data)

    def _process_data(self) -> None:
        while not self._stopped.is_set():
            try:
                item = self._data_queue.get(timeout=1)
            except Empty:
                continue

            key = (item.get("common_address"), item.get("io_address"))
            mapping = self._points_map.get(key)
            if not mapping:
                self._log.debug("Received IEC-104 update for unknown point %s", key)
                continue

            converter = mapping["converter"]
            converted: ConvertedData = converter.convert(mapping, item)

            if converted.telemetry_datapoints_count or converted.attributes_datapoints_count:
                self._log.debug("Converted IEC-104 data: %s", converted)
                StatisticsService.count_connector_message(self.name, 'convertedMessages', count=1)
                self._gateway.send_to_storage(self.get_name(), self.get_id(), converted)

    def _process_commands(self) -> None:
        while not self._stopped.is_set():
            try:
                item = self._command_queue.get(timeout=1)
            except Empty:
                continue

            request = item.get("request")
            originator = item.get("originator")
            try:
                result = self._client.send_command(request)
                if originator:
                    self._gateway.send_rpc_reply(
                        device=originator["device"],
                        req_id=originator["data"]["id"],
                        content=result if result is not None else {"success": True},
                    )
            except Exception as e:  # pragma: no cover - defensive logging
                self._log.exception("Failed to send IEC-104 command: %s", e)
                if originator:
                    self._gateway.send_rpc_reply(
                        device=originator["device"],
                        req_id=originator["data"]["id"],
                        content={"error": str(e)},
                        success_sent=False,
                    )
    # endregion
