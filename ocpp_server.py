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

Server-initiated messages (Server -> CP):
  RemoteStartTransaction  -> {status: Accepted/Rejected}
  RemoteStopTransaction   -> {status: Accepted/Rejected}
  Reset                   -> {status: Accepted/Rejected}
"""
import asyncio
import json
import datetime
import uuid
import websockets
import state
import db


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
    now = _now()
    connector_id = str(payload.get("connectorId", 0))
    cp = state.chargers.setdefault(cp_id, {"connectors": {}})
    # Use .update() so existing fields (transaction_id, meter_latest_wh) are preserved
    conn = cp.setdefault("connectors", {}).setdefault(connector_id, {})
    conn.update({
        "status": payload.get("status", "Unknown"),
        "errorCode": payload.get("errorCode", "NoError"),
        "timestamp": payload.get("timestamp", now),
    })
    _broadcast_charger(cp_id)
    return {}


def _handle_authorize(cp_id: str, payload: dict) -> dict:
    return {"idTagInfo": {"status": "Accepted"}}


def _handle_start_transaction(cp_id: str, payload: dict) -> dict:
    connector_id = str(payload.get("connectorId", 1))
    id_tag = payload.get("idTag", "")
    meter_start = int(payload.get("meterStart", 0))
    timestamp = payload.get("timestamp", _now())

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

    _broadcast_charger(cp_id)
    db.upsert_transaction(tid, state.transactions[tid])
    return {"idTagInfo": {"status": "Accepted"}, "transactionId": tid}


def _handle_stop_transaction(cp_id: str, payload: dict) -> dict:
    tid = int(payload.get("transactionId", 0))
    meter_stop = int(payload.get("meterStop", 0))
    timestamp = payload.get("timestamp", _now())
    reason = payload.get("reason", "Local")

    txn = state.transactions.get(tid)
    if txn:
        txn["meter_stop"] = meter_stop
        txn["stop_time"] = timestamp
        txn["reason"] = reason
        txn["energy_wh"] = meter_stop - txn["meter_start"]

        connector_id = txn["connector_id"]
        cp = state.chargers.get(cp_id, {})
        conn = cp.get("connectors", {}).get(connector_id, {})
        conn.pop("transaction_id", None)
        conn.pop("txn_start_time", None)
        conn["meter_latest_wh"] = meter_stop

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


def _handle_meter_values(cp_id: str, payload: dict) -> dict:
    connector_id = str(payload.get("connectorId", 1))
    tid = payload.get("transactionId")
    if tid is not None:
        tid = int(tid)

    cp = state.chargers.get(cp_id, {})
    conn = cp.get("connectors", {}).get(connector_id, {})

    for meter_value in payload.get("meterValue", []):
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
                break

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
    else:
        return json.dumps([4, uid, "NotImplemented", "Action '{}' is not supported".format(action), {}])

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
                fut = _pending_calls.pop(uid, None)
                if fut and not fut.done():
                    fut.set_exception(Exception(f"CallError {msg[2]}: {msg[3]}"))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        state.chargers.pop(cp_id, None)
        state.ws_connections.pop(cp_id, None)
        state.broadcast({
            "type": "log", "ts": _now(), "cp_id": cp_id,
            "direction": "SYS", "action": "Disconnected", "payload": {},
        })
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
        payload = {"connectorId": connector_id}
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
        payload = {"type": (extra or {}).get("reset_type", "Hard")}
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

async def run_server(host: str = "0.0.0.0", port: int = 9000):
    async with websockets.serve(
        handle_charger, host, port,
        subprotocols=["ocpp1.6"],
        ping_interval=None,
    ):
        print(f"  OCPP WebSocket : ws://{host}:{port}/ocpp/{{CP_ID}}")
        await asyncio.Future()
