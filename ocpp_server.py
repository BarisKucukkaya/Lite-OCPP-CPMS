"""
OCPP 1.6J WebSocket server.

Charge points connect to:  ws://<host>:9000/ocpp/<CP_ID>

Handled messages (CP -> Server):
  BootNotification      -> Accepted
  Heartbeat             -> currentTime
  StatusNotification    -> {} (connector status stored in state)
  Authorize             -> idTagInfo Accepted
  StartTransaction      -> idTagInfo Accepted + transactionId
  StopTransaction       -> idTagInfo Accepted
  MeterValues           -> {}
  DiagnosticsStatusNotification -> {}
  FirmwareStatusNotification    -> {}

Server-initiated messages (Server -> CP):
  RemoteStartTransaction  -> {status: Accepted/Rejected}
  RemoteStopTransaction   -> {status: Accepted/Rejected}
  Reset                   -> {status: Accepted/Rejected}
  TriggerMessage          -> {status: Accepted/Rejected/NotImplemented}
"""
import asyncio
import json
import datetime
import logging
import os
import uuid
import websockets
import state
import db

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ocpp_server.log")

logging.basicConfig(
    filename=LOG_FILE,
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)
# Escalate the websockets library's own handshake logger so the raw HTTP
# request line, headers, and handshake exceptions land in the log file too.
logging.getLogger("websockets.server").setLevel(logging.DEBUG)

logger = logging.getLogger("ocpp_server")


class OcppError(Exception):
    """Raised by a _handle_* function to signal a spec-compliant CallError response."""
    def __init__(self, code: str, description: str = ""):
        self.code = code
        self.description = description
        super().__init__("{}: {}".format(code, description))


VALID_CONNECTOR_STATUSES = {
    "Available", "Preparing", "Charging", "SuspendedEVSE", "SuspendedEV",
    "Finishing", "Reserved", "Unavailable", "Faulted",
}
VALID_ERROR_CODES = {
    "ConnectorLockFailure", "EVCommunicationError", "GroundFailure",
    "HighTemperature", "InternalError", "LocalListConflict", "NoError",
    "OtherError", "OverCurrentFailure", "PowerMeterFailure",
    "PowerSwitchFailure", "ReaderFailure", "ResetFailure", "UnderVoltage",
    "OverVoltage", "WeakSignal",
}
VALID_STOP_REASONS = {
    "EmergencyStop", "EVDisconnected", "HardReset", "Local", "Other",
    "PowerLoss", "Reboot", "Remote", "SoftReset", "UnlockCommand", "DeAuthorized",
}
VALID_RESET_TYPES = {"Hard", "Soft"}
VALID_MESSAGE_TRIGGERS = {
    "BootNotification", "DiagnosticsStatusNotification",
    "FirmwareStatusNotification", "Heartbeat", "MeterValues", "StatusNotification",
}
SOC_AUTO_STOP_THRESHOLD = 100


def _now() -> str:
    return datetime.datetime.utcnow().isoformat() + "Z"


def _uid() -> str:
    return str(uuid.uuid4())[:8]


def _charger_snapshot(cp_id: str) -> dict:
    info = state.chargers.get(cp_id, {})
    return {
        "vendor": info.get("vendor", ""),
        "model": info.get("model", ""),
        "serial": info.get("serial", ""),
        "firmware": info.get("firmware", ""),
        "last_heartbeat": info.get("last_heartbeat"),
        "connected_at": info.get("connected_at"),
        "connectors": {k: dict(v) for k, v in info.get("connectors", {}).items()},
    }


def _broadcast_charger(cp_id: str):
    snap = _charger_snapshot(cp_id)
    state.broadcast({"type": "charger_update", "cp_id": cp_id, **snap})


# ---------------------------------------------------------------------------
# OCPP message handlers (CP -> Server)
# ---------------------------------------------------------------------------

def _handle_boot(cp_id: str, payload: dict) -> dict:
    now = _now()
    cp = state.chargers.setdefault(cp_id, {"connectors": {}})
    cp["vendor"] = payload.get("chargePointVendor", "")
    cp["model"] = payload.get("chargePointModel", "")
    cp["serial"] = payload.get("chargePointSerialNumber", "")
    cp["firmware"] = payload.get("firmwareVersion", "")
    _broadcast_charger(cp_id)
    return {"currentTime": now, "interval": 20, "status": "Accepted"}


def _handle_heartbeat(cp_id: str, payload: dict) -> dict:
    now = _now()
    if cp_id in state.chargers:
        state.chargers[cp_id]["last_heartbeat"] = now
    _broadcast_charger(cp_id)
    return {"currentTime": now}


def _handle_status_notification(cp_id: str, payload: dict) -> dict:
    status = payload.get("status", "Unknown")
    error_code = payload.get("errorCode", "NoError")
    if status not in VALID_CONNECTOR_STATUSES:
        raise OcppError("PropertyConstraintViolation", "Invalid status value: '{}'".format(status))
    if error_code not in VALID_ERROR_CODES:
        raise OcppError("PropertyConstraintViolation", "Invalid errorCode value: '{}'".format(error_code))

    now = _now()
    connector_id = str(payload.get("connectorId", 0))
    cp = state.chargers.setdefault(cp_id, {"connectors": {}})
    # Use .update() so existing fields (transaction_id, meter_latest_wh) are preserved
    conn = cp.setdefault("connectors", {}).setdefault(connector_id, {})
    conn.update({
        "status": status,
        "errorCode": error_code,
        "timestamp": payload.get("timestamp", now),
    })
    _broadcast_charger(cp_id)
    return {}


def _handle_authorize(cp_id: str, payload: dict) -> dict:
    id_tag = payload.get("idTag", "")
    if not db.is_card_authorized(id_tag):
        return {"idTagInfo": {"status": "Invalid"}}
    return {"idTagInfo": {"status": "Accepted"}}


def _handle_start_transaction(cp_id: str, payload: dict) -> dict:
    connector_id = str(payload.get("connectorId", 1))
    id_tag = payload.get("idTag", "")
    meter_start = int(payload.get("meterStart", 0))
    timestamp = payload.get("timestamp", _now())

    if not db.is_card_authorized(id_tag):
        return {"idTagInfo": {"status": "Invalid"}, "transactionId": 0}

    tid = state.next_transaction_id()
    state.transactions[tid] = {
        "cp_id": cp_id,
        "connector_id": connector_id,
        "id_tag": id_tag,
        "meter_start": meter_start,
        "meter_stop": None,
        "start_time": timestamp,
        "stop_time": None,
        "energy_wh": 0,
        "reason": None,
    }

    cp = state.chargers.setdefault(cp_id, {"connectors": {}})
    conn = cp.setdefault("connectors", {}).setdefault(connector_id, {})
    conn["transaction_id"] = tid
    conn["meter_latest_wh"] = meter_start
    conn["txn_start_time"] = timestamp
    conn["_auto_stop_sent"] = False
    conn["soc_percent"] = None
    conn["last_id_tag"] = id_tag

    _broadcast_charger(cp_id)
    db.upsert_transaction(tid, state.transactions[tid])
    return {"idTagInfo": {"status": "Accepted"}, "transactionId": tid}


def _handle_stop_transaction(cp_id: str, payload: dict) -> dict:
    reason = payload.get("reason", "Local")
    if reason not in VALID_STOP_REASONS:
        raise OcppError("PropertyConstraintViolation", "Invalid reason value: '{}'".format(reason))

    tid = int(payload.get("transactionId", 0))
    meter_stop = int(payload.get("meterStop", 0))
    timestamp = payload.get("timestamp", _now())

    txn = state.transactions.get(tid)
    if txn:
        txn["meter_stop"] = meter_stop
        txn["stop_time"] = timestamp
        txn["reason"] = reason
        txn["energy_wh"] = meter_stop - txn["meter_start"]

        connector_id = txn["connector_id"]
        cp = state.chargers.get(cp_id, {})
        conn = cp.get("connectors", {}).get(connector_id, {})
        # Pop transaction_id BEFORE processing transactionData so a SoC=100 sample in
        # transactionData can't trigger a redundant auto-stop for a session already closing.
        conn.pop("transaction_id", None)
        conn.pop("txn_start_time", None)
        conn["meter_latest_wh"] = meter_stop
        conn["_auto_stop_sent"] = False

        transaction_data = payload.get("transactionData")
        if transaction_data:
            _process_meter_value_list(cp_id, connector_id, conn, transaction_data, tid)

    _broadcast_charger(cp_id)
    if txn:
        db.upsert_transaction(tid, txn)
    state.broadcast({
        "type": "transaction_closed",
        "transaction_id": tid,
        "cp_id": cp_id,
        "energy_wh": txn["energy_wh"] if txn else 0,
        "reason": reason,
        "stop_time": timestamp,
    })
    return {"idTagInfo": {"status": "Accepted"}}


def _on_auto_stop_done(task: "asyncio.Task", cp_id: str, connector_id: str) -> None:
    """Done-callback for the fire-and-forget auto RemoteStopTransaction task.
    Consumes any exception so asyncio doesn't warn about it, and logs the failure."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        state.broadcast({
            "type": "log", "ts": _now(), "cp_id": cp_id,
            "direction": "SYS", "action": "AutoStopFailed",
            "payload": {"connector_id": connector_id, "error": str(exc)},
        })


def _process_meter_value_list(cp_id: str, connector_id: str, conn: dict,
                               meter_value_list: list, tid) -> None:
    """
    Shared sample-processing logic for MeterValues.meterValue and
    StopTransaction.transactionData (same array shape).
    """
    for meter_value in meter_value_list:
        for sample in meter_value.get("sampledValue", []):
            measurand = sample.get("measurand", "Energy.Active.Import.Register")
            unit = sample.get("unit", "Wh")
            if measurand in ("Energy.Active.Import.Register", "") or "measurand" not in sample:
                try:
                    value_raw = float(sample.get("value", 0))
                    wh = int(value_raw * 1000) if unit == "kWh" else int(value_raw)
                    conn["meter_latest_wh"] = wh
                    if tid and tid in state.transactions:
                        state.transactions[tid]["energy_wh"] = (
                            wh - state.transactions[tid]["meter_start"]
                        )
                except (ValueError, TypeError):
                    pass
            elif measurand == "Power.Active.Import":
                try:
                    value_raw = float(sample.get("value", 0))
                    conn["power_w"] = int(value_raw * 1000) if unit == "kW" else int(value_raw)
                except (ValueError, TypeError):
                    pass
            elif measurand == "SoC":
                try:
                    soc = int(float(sample.get("value", 0)))
                    conn["soc_percent"] = soc
                    if (soc >= SOC_AUTO_STOP_THRESHOLD
                            and conn.get("transaction_id") is not None
                            and not conn.get("_auto_stop_sent")):
                        conn["_auto_stop_sent"] = True
                        state.broadcast({
                            "type": "log", "ts": _now(), "cp_id": cp_id,
                            "direction": "SYS", "action": "AutoStopTriggered",
                            "payload": {"connector_id": connector_id, "soc": soc},
                        })
                        task = asyncio.ensure_future(
                            remote_action(cp_id, "RemoteStopTransaction", int(connector_id))
                        )
                        task.add_done_callback(
                            lambda t, cp=cp_id, cid=connector_id: _on_auto_stop_done(t, cp, cid)
                        )
                except (ValueError, TypeError):
                    pass


def _handle_diagnostics_status_notification(cp_id: str, payload: dict) -> dict:
    return {}


def _handle_firmware_status_notification(cp_id: str, payload: dict) -> dict:
    return {}


def _handle_meter_values(cp_id: str, payload: dict) -> dict:
    connector_id = str(payload.get("connectorId", 1))
    tid = payload.get("transactionId")
    if tid is not None:
        tid = int(tid)

    cp = state.chargers.get(cp_id, {})
    conn = cp.get("connectors", {}).get(connector_id, {})

    _process_meter_value_list(cp_id, connector_id, conn, payload.get("meterValue", []), tid)

    _broadcast_charger(cp_id)
    return {}


# ---------------------------------------------------------------------------
# Per-connection handler
# ---------------------------------------------------------------------------

# Pending server-initiated calls: {uid: asyncio.Future}
_pending_calls = {}
_pending_lock = asyncio.Lock() if False else None  # plain dict is fine, same loop


async def _process_cp_call(cp_id: str, uid: str, action: str, payload: dict) -> str:
    """Handle a Call [2] from the charge point, return CallResult/CallError string."""
    state.broadcast({
        "type": "log",
        "ts": _now(),
        "cp_id": cp_id,
        "direction": "IN",
        "action": action,
        "unique_id": uid,
        "payload": payload,
    })

    try:
        if action == "BootNotification":
            resp = _handle_boot(cp_id, payload)
        elif action == "Heartbeat":
            resp = _handle_heartbeat(cp_id, payload)
        elif action == "StatusNotification":
            resp = _handle_status_notification(cp_id, payload)
        elif action == "Authorize":
            resp = _handle_authorize(cp_id, payload)
        elif action == "StartTransaction":
            resp = _handle_start_transaction(cp_id, payload)
        elif action == "StopTransaction":
            resp = _handle_stop_transaction(cp_id, payload)
        elif action == "MeterValues":
            resp = _handle_meter_values(cp_id, payload)
        elif action == "DiagnosticsStatusNotification":
            resp = _handle_diagnostics_status_notification(cp_id, payload)
        elif action == "FirmwareStatusNotification":
            resp = _handle_firmware_status_notification(cp_id, payload)
        else:
            state.broadcast({"type": "log", "ts": _now(), "cp_id": cp_id,
                "direction": "IN", "action": action, "unique_id": uid, "payload": payload})
            return json.dumps([4, uid, "NotImplemented", "Action '{}' is not supported".format(action), {}])
    except OcppError as e:
        state.broadcast({
            "type": "log", "ts": _now(), "cp_id": cp_id,
            "direction": "OUT", "action": action + "Error",
            "unique_id": uid, "payload": {"code": e.code, "description": e.description},
        })
        return json.dumps([4, uid, e.code, e.description, {}])
    except Exception as e:
        state.broadcast({
            "type": "log", "ts": _now(), "cp_id": cp_id,
            "direction": "OUT", "action": action + "Error",
            "unique_id": uid, "payload": {"code": "InternalError", "description": str(e)},
        })
        return json.dumps([4, uid, "InternalError", str(e), {}])

    state.broadcast({
        "type": "log",
        "ts": _now(),
        "cp_id": cp_id,
        "direction": "OUT",
        "action": action + "Response",
        "unique_id": uid,
        "payload": resp,
    })
    return json.dumps([3, uid, resp])


async def handle_charger(websocket, path: str):
    cp_id = path.rstrip("/").split("/")[-1] or "UNKNOWN"

    state.chargers[cp_id] = {
        "connectors": {},
        "last_heartbeat": None,
        "connected_at": _now(),
        "vendor": "", "model": "", "serial": "", "firmware": "",
    }
    state.ws_connections[cp_id] = websocket

    state.broadcast({
        "type": "log", "ts": _now(), "cp_id": cp_id,
        "direction": "SYS", "action": "Connected", "payload": {},
    })
    logger.info("Charger '%s' completed WS handshake and registered (remote=%s)", cp_id, websocket.remote_address)
    _broadcast_charger(cp_id)

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                continue

            if not isinstance(msg, list) or len(msg) < 3:
                continue

            msg_type = msg[0]

            if msg_type == 2:
                # Call from charge point
                uid = msg[1]
                action = msg[2]
                payload = msg[3] if len(msg) > 3 else {}
                response = await _process_cp_call(cp_id, uid, action, payload)
                await websocket.send(response)

            elif msg_type == 3:
                # CallResult — response to a server-initiated call
                uid = msg[1]
                result_payload = msg[2] if len(msg) > 2 else {}
                fut = _pending_calls.pop(uid, None)
                if fut and not fut.done():
                    fut.set_result(result_payload)

            elif msg_type == 4:
                # CallError — charge point rejected a server-initiated call
                uid = msg[1]
                error_code = msg[2] if len(msg) > 2 else "GenericError"
                error_description = msg[3] if len(msg) > 3 else ""
                fut = _pending_calls.pop(uid, None)
                if fut and not fut.done():
                    fut.set_exception(Exception(f"CallError {error_code}: {error_description}"))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        cp = state.chargers.get(cp_id, {})
        for conn in cp.get("connectors", {}).values():
            tid = conn.get("transaction_id")
            if tid is not None:
                txn = state.transactions.get(int(tid))
                if txn and txn.get("stop_time") is None:
                    now = _now()
                    meter_stop = conn.get("meter_latest_wh", txn["meter_start"])
                    txn["meter_stop"] = meter_stop
                    txn["stop_time"] = now
                    txn["reason"] = "PowerLoss"
                    txn["energy_wh"] = meter_stop - txn["meter_start"]
                    db.upsert_transaction(int(tid), txn)
                    state.broadcast({
                        "type": "transaction_closed",
                        "transaction_id": int(tid),
                        "cp_id": cp_id,
                        "energy_wh": txn["energy_wh"],
                        "reason": "PowerLoss",
                        "stop_time": now,
                    })
        state.chargers.pop(cp_id, None)
        state.ws_connections.pop(cp_id, None)
        state.broadcast({
            "type": "log", "ts": _now(), "cp_id": cp_id,
            "direction": "SYS", "action": "Disconnected", "payload": {},
        })
        logger.info("Charger '%s' disconnected", cp_id)
        state.broadcast({"type": "charger_disconnect", "cp_id": cp_id})


# ---------------------------------------------------------------------------
# Server-initiated OCPP calls
# ---------------------------------------------------------------------------

async def remote_action(cp_id: str, action: str, connector_id: int = 1, extra: dict = None) -> dict:
    """
    Send a server-initiated OCPP command to a charge point.
    Returns the charge point's response payload or raises on error/timeout.
    """
    ws = state.ws_connections.get(cp_id)
    if ws is None:
        raise Exception("Charge point '{}' is not connected".format(cp_id))

    uid = _uid()
    if action == "RemoteStartTransaction":
        id_tag = (extra or {}).get("id_tag")
        if not id_tag:
            raise Exception("idTag is required for RemoteStartTransaction")
        payload = {"connectorId": connector_id, "idTag": id_tag}
    elif action == "RemoteStopTransaction":
        cp = state.chargers.get(cp_id, {})
        conn = cp.get("connectors", {}).get(str(connector_id), {})
        tid = conn.get("transaction_id")
        if tid is None:
            raise Exception(
                "No active transaction on connector {} of '{}'".format(connector_id, cp_id)
            )
        payload = {"transactionId": tid}
    elif action == "Reset":
        reset_type = (extra or {}).get("reset_type", "Hard")
        if reset_type not in VALID_RESET_TYPES:
            raise Exception("Invalid reset type: '{}' (must be Hard or Soft)".format(reset_type))
        payload = {"type": reset_type}
    elif action == "TriggerMessage":
        requested_message = (extra or {}).get("requested_message")
        if requested_message not in VALID_MESSAGE_TRIGGERS:
            raise Exception("Invalid requested_message: '{}'".format(requested_message))
        payload = {"requestedMessage": requested_message}
        if requested_message in ("StatusNotification", "MeterValues"):
            payload["connectorId"] = connector_id
    else:
        raise Exception("Unknown action: {}".format(action))

    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    _pending_calls[uid] = fut

    state.broadcast({
        "type": "log", "ts": _now(), "cp_id": cp_id,
        "direction": "OUT", "action": action,
        "unique_id": uid, "payload": payload,
    })

    await ws.send(json.dumps([2, uid, action, payload]))

    try:
        result = await asyncio.wait_for(fut, timeout=10)
        state.broadcast({
            "type": "log", "ts": _now(), "cp_id": cp_id,
            "direction": "IN", "action": action + "Response",
            "unique_id": uid, "payload": result,
        })
        return result
    except asyncio.TimeoutError:
        _pending_calls.pop(uid, None)
        raise Exception(f"Timeout waiting for {action} response from '{cp_id}'")


# ---------------------------------------------------------------------------
# Server startup
# ---------------------------------------------------------------------------

class LoggingProtocol(websockets.WebSocketServerProtocol):
    """Logs every inbound TCP connection and HTTP handshake attempt to disk,
    including ones that fail before handle_charger() is ever entered."""

    def connection_made(self, transport):
        peer = transport.get_extra_info("peername")
        logger.info("TCP connection accepted from %s", peer)
        super().connection_made(transport)

    async def process_request(self, path, request_headers):
        logger.info(
            "HTTP request from %s: path=%r headers=%s",
            self.remote_address, path, dict(request_headers.raw_items()),
        )
        return await super().process_request(path, request_headers)


async def run_server(host: str = "0.0.0.0", port: int = 9000):
    logger.info("Starting OCPP WebSocket server on %s:%s", host, port)
    try:
        async with websockets.serve(
            handle_charger, host, port,
            subprotocols=["ocpp1.6"],
            ping_interval=None,
            create_protocol=LoggingProtocol,
        ):
            print(f"  OCPP WebSocket : ws://{host}:{port}/ocpp/{{CP_ID}}")
            logger.info("OCPP WebSocket server listening on %s:%s", host, port)
            await asyncio.Future()
    except Exception:
        logger.exception("OCPP WebSocket server failed to start/bind on %s:%s", host, port)
        raise
