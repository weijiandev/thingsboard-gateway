"""Client abstractions for the IEC 60870-5-104 connector."""

from __future__ import annotations

import socket
import struct
from abc import ABC, abstractmethod
from datetime import datetime
from importlib import import_module
from threading import Event, Lock, Thread
from time import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

UpdateCallback = Callable[[Dict[str, Any]], None]


class IEC104ClientBase(ABC):
    """Base interface for IEC 60870-5-104 client implementations."""

    def __init__(self, config: Dict[str, Any], logger):
        self._config = config
        self._logger = logger
        self._listener: Optional[UpdateCallback] = None

    def set_listener(self, listener: UpdateCallback) -> None:
        self._listener = listener

    def is_connected(self) -> bool:
        """Return whether the IEC-104 client considers itself connected."""

        return True

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
        self._logger.warning("Dummy IEC-104 client cannot execute command: %s", request)
        return {"success": False, "error": "Dummy IEC-104 client"}


class IEC104ClientFactory:
    """Factory helper that instantiates IEC-104 client implementations."""

    DEFAULT_CLASS_PATH = "thingsboard_gateway.connectors.iec104.client.RemoteIEC104Client"

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


class RemoteIEC104Client(IEC104ClientBase):
    """Client implementation capable of connecting to remote IEC-104 servers."""

    STARTDT_ACT = 0x07
    STARTDT_CON = 0x0B
    STOPDT_ACT = 0x13
    STOPDT_CON = 0x23
    TESTFR_ACT = 0x43
    TESTFR_CON = 0x83

    GENERAL_INTERROGATION = 100

    COMMAND_TYPE_MAP = {
        "single": 45,  # C_SC_NA_1
        "double": 46,  # C_DC_NA_1
        "step-position": 47,  # C_RC_NA_1
        "setpoint-normalized": 48,  # C_SE_NA_1
        "setpoint-scaled": 49,  # C_SE_NB_1
        "setpoint-float": 50,  # C_SE_NC_1
        "bitstring": 51,  # C_BO_NA_1
    }

    COMMAND_TYPES = set(COMMAND_TYPE_MAP.values())

    _MEASUREMENT_SIZES = {
        1: 1,  # M_SP_NA_1 single point (value + quality)
        3: 1,  # M_DP_NA_1 double point (value + quality)
        5: 3,  # M_ST_NA_1 step position + quality
        9: 3,  # M_ME_NA_1 normalized + quality
        11: 3,  # M_ME_NB_1 scaled + quality
        13: 5,  # M_ME_NC_1 short float + quality
        21: 2,  # M_ME_ND_1 normalized without quality
        30: 8,  # M_SP_TB_1 single point with time tag CP56Time2a
        31: 8,  # M_DP_TB_1 double point with time tag
        32: 10,  # M_ST_TB_1 step position with time tag
        33: 12,  # M_BO_TB_1 bit string with time tag
        34: 12,  # M_ME_TD_1 normalized with time tag
        35: 12,  # M_ME_TE_1 scaled with time tag
        36: 14,  # M_ME_TF_1 float with time tag
        37: 10,  # M_IT_TB_1 integrated totals with time tag
    }

    def __init__(self, config: Dict[str, Any], logger):
        super().__init__(config, logger)
        self._host: str = config.get("host", "127.0.0.1")
        self._port: int = int(config.get("port", 2404))
        self._timeout: float = float(config.get("timeout", 10.0))
        self._interrogation_timeout: float = float(config.get("interrogationTimeout", 10.0))
        self._command_timeout: float = float(config.get("commandTimeout", 5.0))
        self._select_before_operate: bool = bool(config.get("selectBeforeOperate", False))
        self._originator_address: int = int(config.get("originatorAddress", 0)) & 0xFF
        self._set_test_bit: bool = bool(config.get("setTestBit", False))

        self._socket: Optional[socket.socket] = None
        self._socket_lock = Lock()
        self._running = Event()
        self._receive_thread: Optional[Thread] = None
        self._buffer = bytearray()

        self._send_seq: int = 0
        self._expected_recv_seq: int = 0

        self._start_confirmed = Event()

        self._interrogation_context: Dict[int, Dict[str, Any]] = {}
        self._command_context: Dict[Tuple[int, int, int], Dict[str, Any]] = {}
        self._context_lock = Lock()

    def is_connected(self) -> bool:
        with self._socket_lock:
            return self._running.is_set() and self._socket is not None

    # region IEC-104 connection lifecycle
    def connect(self) -> None:
        self._logger.debug("Connecting to IEC-104 server %s:%s", self._host, self._port)
        try:
            sock = socket.create_connection((self._host, self._port), self._timeout)
        except OSError as exc:  # pragma: no cover - connection failures depend on environment
            raise ConnectionError(
                f"Failed to connect to IEC-104 server {self._host}:{self._port}: {exc}"
            ) from exc

        sock.settimeout(self._timeout)
        self._socket = sock
        self._running.set()
        self._start_confirmed.clear()
        self._buffer.clear()
        self._send_seq = 0
        self._expected_recv_seq = 0
        self._receive_thread = Thread(target=self._receive_loop, name="IEC104 Receive", daemon=True)
        self._receive_thread.start()

        self._send_u_frame(self.STARTDT_ACT)
        if not self._start_confirmed.wait(self._timeout):
            raise TimeoutError("IEC-104 start data transfer confirmation timeout")

        self._logger.info("Connected to IEC-104 server %s:%s", self._host, self._port)

    def close(self) -> None:
        self._running.clear()
        sock: Optional[socket.socket] = None
        with self._socket_lock:
            if self._socket:
                sock = self._socket
                try:
                    frame = bytearray([0x68, 0x04, self.STOPDT_ACT, 0x00, 0x00, 0x00])
                    sock.sendall(frame)
                except OSError:  # pragma: no cover - best effort during shutdown
                    pass
                self._socket = None

        self._cleanup_socket(sock)

        if self._receive_thread and self._receive_thread.is_alive():
            self._receive_thread.join(timeout=5)
            self._receive_thread = None

    # endregion

    # region public API
    def interrogate(self, device_config: Dict[str, Any], points: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        common_address = device_config.get("commonAddress")
        if common_address is None:
            raise ValueError("Device configuration must define 'commonAddress' for IEC-104 interrogation")

        with self._context_lock:
            ctx = {"event": Event(), "updates": []}
            self._interrogation_context[common_address] = ctx

        self._send_general_interrogation(common_address, device_config.get("interrogationQualifier", 20))

        completed = ctx["event"].wait(self._interrogation_timeout)
        if not completed:
            self._logger.warning("General interrogation timed out for common address %s", common_address)

        with self._context_lock:
            self._interrogation_context.pop(common_address, None)

        return ctx["updates"]

    def send_command(self, request: Dict[str, Any]) -> Any:
        command_type = request.get("command", "single").lower()
        type_id = self.COMMAND_TYPE_MAP.get(command_type)
        if type_id is None:
            raise ValueError(f"Unsupported IEC-104 command type: {command_type}")

        common_address = request.get("common_address") or request.get("commonAddress")
        if common_address is None:
            raise ValueError("IEC-104 command requires 'commonAddress'")

        io_address = request.get("io_address") or request.get("ioAddress")
        if io_address is None:
            raise ValueError("IEC-104 command requires 'ioAddress'")

        select = bool(request.get("select", self._select_before_operate))
        qualifier = int(request.get("qualifier", request.get("qos", request.get("qoi", 0)))) & 0xFF
        value = request.get("value", request.get("state"))

        asdu_payload = self._build_command_asdu(type_id, common_address, io_address, value, select, qualifier)

        key = (type_id, common_address, io_address)
        with self._context_lock:
            ctx = {"event": Event(), "result": None}
            self._command_context[key] = ctx

        self._send_i_frame(asdu_payload)

        if ctx["event"].wait(self._command_timeout):
            with self._context_lock:
                self._command_context.pop(key, None)
            return ctx["result"] if ctx["result"] is not None else {"success": True}

        with self._context_lock:
            self._command_context.pop(key, None)
        self._logger.warning("IEC-104 command timeout for common address %s io %s", common_address, io_address)
        return {"success": False, "error": "Timeout waiting for command confirmation"}

    # endregion

    # region IEC-104 frame handling
    def _receive_loop(self) -> None:
        while self._running.is_set():
            try:
                if not self._socket:
                    break
                chunk = self._socket.recv(4096)
                if not chunk:
                    self._logger.warning("IEC-104 connection closed by remote host")
                    break
                self._buffer.extend(chunk)
                self._process_buffer()
            except (OSError, ConnectionError) as exc:
                if self._running.is_set():
                    self._logger.exception("IEC-104 receive loop error: %s", exc)
                break
            except Exception as exc:  # pragma: no cover - defensive logging
                self._logger.exception("Unexpected IEC-104 receive error: %s", exc)
                break

        self._running.clear()
        with self._socket_lock:
            sock = self._socket
            self._socket = None
        self._cleanup_socket(sock)
        self._start_confirmed.set()

    def _process_buffer(self) -> None:
        while True:
            if len(self._buffer) < 2:
                return
            if self._buffer[0] != 0x68:
                self._buffer.pop(0)
                continue
            length = self._buffer[1]
            if len(self._buffer) < 2 + length:
                return
            frame = bytes(self._buffer[2 : 2 + length])
            del self._buffer[: 2 + length]
            self._handle_frame(frame)

    def _handle_frame(self, frame: bytes) -> None:
        if len(frame) < 4:
            return
        control = frame[:4]
        asdu = frame[4:]
        first_byte = control[0]

        if first_byte & 0x01 == 0:  # I-format
            send_seq = struct.unpack("<H", control[:2])[0] >> 1
            self._expected_recv_seq = (send_seq + 1) % 32768
            self._send_s_frame()

            if asdu:
                self._handle_asdu(asdu)
        elif first_byte & 0x03 == 1:  # S-format
            recv_seq = struct.unpack("<H", control[2:])[0] >> 1
            self._logger.debug("Received IEC-104 S-format acknowledgement: %s", recv_seq)
        else:  # U-format
            self._handle_u_frame(first_byte)

    def _handle_u_frame(self, code: int) -> None:
        if code == self.STARTDT_CON:
            self._logger.debug("IEC-104 start data transfer confirmed")
            self._start_confirmed.set()
        elif code == self.STARTDT_ACT:
            self._logger.debug("IEC-104 start data transfer activation received")
            self._send_u_frame(self.STARTDT_CON)
        elif code == self.STOPDT_ACT:
            self._logger.debug("IEC-104 stop data transfer activation received")
            self._send_u_frame(self.STOPDT_CON)
        elif code == self.TESTFR_ACT:
            self._logger.debug("IEC-104 test frame activation received")
            self._send_u_frame(self.TESTFR_CON)
        elif code == self.TESTFR_CON:
            self._logger.debug("IEC-104 test frame confirmation received")
        else:
            self._logger.debug("IEC-104 unsupported U-format frame: 0x%02X", code)

    def _handle_asdu(self, asdu: bytes) -> None:
        if len(asdu) < 7:
            return

        type_id = asdu[0]
        vsq = asdu[1]
        sq = bool(vsq & 0x80)
        number = vsq & 0x7F or 1
        cause_raw = asdu[2] | (asdu[3] << 8)
        cause = cause_raw & 0x3F
        positive = not bool(asdu[3] & 0x40)
        common_address = asdu[5] | (asdu[6] << 8)

        offset = 7

        if type_id == self.GENERAL_INTERROGATION:
            self._handle_interrogation_response(common_address, cause)
            return

        if type_id in self.COMMAND_TYPES:
            self._handle_command_response(type_id, common_address, cause, positive, asdu, offset, number, sq)
            return

        entry_size = self._MEASUREMENT_SIZES.get(type_id)
        if entry_size is None:
            self._logger.debug("Unsupported IEC-104 ASDU type %s", type_id)
            return

        entries, _ = self._extract_information_objects(asdu, offset, number, entry_size, sq)
        timestamp = int(time() * 1000)
        for io_address, data in entries:
            value, quality, ts = self._decode_information_element(type_id, data, timestamp)
            if value is None:
                continue
            update = {
                "common_address": common_address,
                "io_address": io_address,
                "value": value,
                "quality": quality,
                "ts": ts,
                "cause": cause,
                "positive": positive,
            }
            self._register_update(update)

    def _handle_interrogation_response(self, common_address: int, cause: int) -> None:
        with self._context_lock:
            ctx = self._interrogation_context.get(common_address)
        if not ctx:
            return

        if cause == 10:  # activation termination
            ctx["event"].set()

    def _handle_command_response(
        self,
        type_id: int,
        common_address: int,
        cause: int,
        positive: bool,
        asdu: bytes,
        offset: int,
        number: int,
        sq: bool,
    ) -> None:
        entries, _ = self._extract_information_objects(asdu, offset, number, 1, sq)
        if cause not in (7, 10):
            return

        for io_address, _ in entries:
            key = (type_id, common_address, io_address)
            with self._context_lock:
                ctx = self._command_context.get(key)
                if ctx:
                    ctx["result"] = {"success": positive, "cause": cause}
                    ctx["event"].set()

    def _register_update(self, update: Dict[str, Any]) -> None:
        with self._context_lock:
            ctx = self._interrogation_context.get(update["common_address"])
            if ctx is not None:
                ctx["updates"].append(update)
        self._emit_update(update)

    def _send_general_interrogation(self, common_address: int, qualifier: int) -> None:
        qualifier &= 0xFF
        asdu = bytearray()
        asdu.append(self.GENERAL_INTERROGATION)
        asdu.append(0x01)
        cause = 6 | (0x80 if self._set_test_bit else 0)
        asdu.extend(struct.pack("<H", cause))
        asdu.append(self._originator_address)
        asdu.extend(struct.pack("<H", common_address))
        asdu.extend(struct.pack("<I", 0)[:3])
        asdu.append(qualifier or 20)
        self._send_i_frame(bytes(asdu))

    def _build_command_asdu(
        self,
        type_id: int,
        common_address: int,
        io_address: int,
        value: Any,
        select: bool,
        qualifier: int,
    ) -> bytes:
        asdu = bytearray()
        asdu.append(type_id)
        asdu.append(0x01)
        cause = 6 | (0x80 if self._set_test_bit else 0)
        asdu.extend(struct.pack("<H", cause))
        asdu.append(self._originator_address)
        asdu.extend(struct.pack("<H", common_address))
        asdu.extend(struct.pack("<I", io_address)[:3])

        if type_id in (45, 46):
            value_bit = 0
            if type_id == 45:
                value_bit = 1 if bool(value) else 0
            elif type_id == 46:
                value_bit = int(value) & 0x03 if value is not None else 0
            control_byte = value_bit | (0x80 if select else 0)
            asdu.append(control_byte)
        elif type_id == 47:
            value_bit = int(value) & 0x1F if value is not None else 0
            asdu.append(value_bit | (0x80 if select else 0))
        elif type_id == 48:
            val = int(float(value) * 32767) if value is not None else 0
            asdu.extend(struct.pack("<h", val))
            asdu.append(qualifier & 0xFF)
        elif type_id == 49:
            val = int(value) if value is not None else 0
            asdu.extend(struct.pack("<h", val))
            asdu.append(qualifier & 0xFF)
        elif type_id == 50:
            val = float(value) if value is not None else 0.0
            asdu.extend(struct.pack("<f", val))
            asdu.append(qualifier & 0xFF)
        elif type_id == 51:
            bitstring = int(value) if value is not None else 0
            asdu.extend(struct.pack("<I", bitstring))
            asdu.append(qualifier & 0xFF)
        else:  # pragma: no cover - guarded by caller
            raise ValueError(f"Unsupported IEC-104 command type id {type_id}")

        return bytes(asdu)

    def _send_i_frame(self, asdu: bytes) -> None:
        with self._socket_lock:
            if not self._socket:
                raise ConnectionError("IEC-104 socket is not available")

            send_seq = (self._send_seq << 1) & 0xFFFF
            recv_seq = (self._expected_recv_seq << 1) & 0xFFFF
            control = struct.pack("<HH", send_seq, recv_seq)
            length = len(asdu) + 4
            frame = bytearray()
            frame.append(0x68)
            frame.append(length)
            frame.extend(control)
            frame.extend(asdu)
            try:
                self._socket.sendall(frame)
            except OSError as exc:
                self._handle_send_error(exc)
                return
            self._send_seq = (self._send_seq + 1) % 32768

    def _send_s_frame(self) -> None:
        if not self._socket:
            return

        with self._socket_lock:
            if not self._socket:
                return
            recv_seq = (self._expected_recv_seq << 1) & 0xFFFF
            frame = bytearray([0x68, 0x04, 0x01, 0x00])
            frame.extend(struct.pack("<H", recv_seq))
            try:
                self._socket.sendall(frame)
            except OSError as exc:  # pragma: no cover - acknowledgement best effort
                self._handle_send_error(exc)

    def _send_u_frame(self, code: int) -> None:
        with self._socket_lock:
            if not self._socket:
                raise ConnectionError("IEC-104 socket is not available")

            frame = bytearray([0x68, 0x04, code, 0x00, 0x00, 0x00])
            try:
                self._socket.sendall(frame)
            except OSError as exc:
                self._handle_send_error(exc)

    # endregion

    def _cleanup_socket(self, sock: Optional[socket.socket]) -> None:
        if not sock:
            return

        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            sock.close()
        except OSError:
            pass

    def _handle_send_error(self, exc: OSError) -> None:
        self._logger.warning("IEC-104 socket send failed: %s", exc)
        sock = self._socket
        self._socket = None
        self._running.clear()
        self._start_confirmed.set()
        self._send_seq = 0
        self._expected_recv_seq = 0
        self._cleanup_socket(sock)
        raise ConnectionError("IEC-104 socket send failed") from exc

    # region decoding helpers
    @staticmethod
    def _decode_cp56time2a(data: bytes) -> Optional[int]:
        if len(data) != 7:
            return None

        milli = data[0] | (data[1] << 8)
        minute = data[2] & 0x3F
        hour = data[3] & 0x1F
        day = data[4] & 0x1F or 1
        month = (data[5] & 0x0F) or 1
        year = data[6] & 0x7F
        try:
            dt = datetime(
                year=2000 + year,
                month=month,
                day=day,
                hour=hour,
                minute=minute,
                second=milli // 1000,
                microsecond=(milli % 1000) * 1000,
            )
        except ValueError:
            return None
        return int(dt.timestamp() * 1000)

    def _decode_information_element(self, type_id: int, data: bytes, fallback_ts: int) -> Tuple[Any, int, int]:
        quality = 0
        timestamp = fallback_ts

        if type_id in (30, 31, 32, 33, 34, 35, 36, 37):
            if len(data) < 7:
                return None, quality, fallback_ts
            payload = data[:-7]
            ts = self._decode_cp56time2a(data[-7:])
            if ts is not None:
                timestamp = ts
        else:
            payload = data

        if not payload:
            return None, quality, timestamp

        if type_id in (1, 30):
            quality = payload[0] & 0xF0
            value = bool(payload[0] & 0x01)
        elif type_id in (3, 31):
            quality = payload[0] & 0xF0
            value = payload[0] & 0x03
        elif type_id in (5, 32):
            if len(payload) < 2:
                return None, quality, timestamp
            value = payload[0] & 0x1F
            quality = payload[1]
        elif type_id in (9, 34):
            if len(payload) < 2:
                return None, quality, timestamp
            value = struct.unpack("<h", payload[:2])[0] / 32767.0
            quality = payload[2] if len(payload) > 2 else 0
        elif type_id in (11, 35):
            if len(payload) < 2:
                return None, quality, timestamp
            value = struct.unpack("<h", payload[:2])[0]
            quality = payload[2] if len(payload) > 2 else 0
        elif type_id in (13, 36):
            if len(payload) < 4:
                return None, quality, timestamp
            value = struct.unpack("<f", payload[:4])[0]
            quality = payload[4] if len(payload) > 4 else 0
        elif type_id == 21:
            if len(payload) < 2:
                return None, quality, timestamp
            value = struct.unpack("<h", payload[:2])[0] / 32767.0
        elif type_id == 33:
            if len(payload) < 5:
                return None, quality, timestamp
            value = int.from_bytes(payload[:4], byteorder="little", signed=False)
            quality = payload[4]
        elif type_id == 37:
            if len(payload) < 5:
                return None, quality, timestamp
            value = struct.unpack("<i", payload[:4])[0]
            quality = payload[4]
        else:
            value = payload

        return value, quality, timestamp

    def _extract_information_objects(
        self, asdu: bytes, offset: int, number: int, element_size: int, sq: bool
    ) -> Tuple[List[Tuple[int, bytes]], int]:
        entries: List[Tuple[int, bytes]] = []
        if sq:
            if len(asdu) < offset + 3:
                return entries, len(asdu)
            io_base = asdu[offset] | (asdu[offset + 1] << 8) | (asdu[offset + 2] << 16)
            offset += 3
            for index in range(number):
                start = offset + index * element_size
                end = start + element_size
                if end > len(asdu):
                    return [], len(asdu)
                entries.append((io_base + index, asdu[start:end]))
            offset += number * element_size
        else:
            for _ in range(number):
                if len(asdu) < offset + 3:
                    return entries, len(asdu)
                io_address = asdu[offset] | (asdu[offset + 1] << 8) | (asdu[offset + 2] << 16)
                offset += 3
                end = offset + element_size
                if end > len(asdu):
                    return [], len(asdu)
                entries.append((io_address, asdu[offset:end]))
                offset = end
        return entries, offset

    # endregion
