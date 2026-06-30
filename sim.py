"""
OCPP 1.6J Charge Point Simulator

Simulates a charge point that:
  1. Connects and sends BootNotification
  2. Sends StatusNotification (Available) for connectors 0 and 1
  3. Sends Heartbeat every 20 s (matching server interval)
  4. Listens for RemoteStartTransaction / RemoteStopTransaction from server
     and runs the full OCPP transaction lifecycle:
     Preparing -> Charging -> StartTransaction -> (MeterValues every 5s)
     -> RemoteStop -> StopTransaction -> Finishing -> Available

Usage:
    python3 sim.py                        # CP001, connects to localhost:9000
    python3 sim.py CP002                  # custom ID
    python3 sim.py CP002 192.168.1.10     # custom ID + server IP
"""
import asyncio
import json
import sys
import uuid
import datetime
import websockets

CP_ID = sys.argv[1] if len(sys.argv) > 1 else "CP001"
HOST  = sys.argv[2] if len(sys.argv) > 2 else "localhost"
PORT  = 9000
URL   = "ws://{}:{}/ocpp/{}".format(HOST, PORT, CP_ID)

HEARTBEAT_INTERVAL = 20  # seconds — must match BootNotification response interval


def _now():
    return datetime.datetime.utcnow().isoformat() + "Z"


def _uid():
    return str(uuid.uuid4())[:8]


# ---------------------------------------------------------------------------
# Pending call futures: uid -> Future
# Each send_call() registers a future here; _receiver() resolves it.
# ---------------------------------------------------------------------------
_pending = {}

# Active charging sessions per connector: {connector_id: {"task", "meter_wh", "transaction_id"}}
_active_sessions = {}


async def send_call(ws, action, payload):
    uid = _uid()
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    _pending[uid] = fut
    await ws.send(json.dumps([2, uid, action, payload]))
    print("  --> {}".format(action))
    try:
        msg = await asyncio.wait_for(fut, timeout=10)
    except asyncio.TimeoutError:
        _pending.pop(uid, None)
        print("  !!! Timeout waiting for {}Response".format(action))
        return None
    if msg[0] == 3:
        print("  <-- {}Response: {}".format(action, json.dumps(msg[2])))
    elif msg[0] == 4:
        print("  <-- ERROR ({}): {}".format(msg[2], msg[3]))
    return msg


async def send_status(ws, connector_id, status, error_code=None):
    if error_code is None:
        error_code = "GroundFailure" if status == "Faulted" else "NoError"
    await send_call(ws, "StatusNotification", {
        "connectorId": connector_id,
        "errorCode": error_code,
        "status": status,
        "timestamp": _now(),
    })


# ---------------------------------------------------------------------------
# MeterValues loop — runs while connector is Charging
# ---------------------------------------------------------------------------

async def _meter_values_loop(ws, connector_id, transaction_id):
    """Send MeterValues every 5 s, simulating ~11 kW charging (55 Wh per 5 s tick)."""
    session = _active_sessions.get(connector_id, {})
    while True:
        await asyncio.sleep(5)
        session["meter_wh"] = session.get("meter_wh", 0) + 55
        kwh = round(session["meter_wh"] / 1000.0, 4)
        await send_call(ws, "MeterValues", {
            "connectorId": connector_id,
            "transactionId": transaction_id,
            "meterValue": [{
                "timestamp": _now(),
                "sampledValue": [{
                    "value": str(kwh),
                    "measurand": "Energy.Active.Import.Register",
                    "unit": "kWh",
                    "context": "Sample.Periodic",
                    "format": "Raw",
                    "location": "Outlet",
                }],
            }],
        })


# ---------------------------------------------------------------------------
# Handle server-initiated calls (RemoteStart / RemoteStop / Reset)
# ---------------------------------------------------------------------------

async def _handle_server_call(ws, uid, action, payload):
    if action == "RemoteStartTransaction":
        connector_id = payload.get("connectorId", 1)
        await ws.send(json.dumps([3, uid, {"status": "Accepted"}]))
        print("  <-- RemoteStartTransaction (connector {}) -> Accepted".format(connector_id))

        await asyncio.sleep(1)
        await send_status(ws, connector_id, "Preparing")
        await asyncio.sleep(2)
        await send_status(ws, connector_id, "Charging")

        # Send StartTransaction and get transactionId from server
        resp = await send_call(ws, "StartTransaction", {
            "connectorId": connector_id,
            "idTag": "SIM001",
            "meterStart": 0,
            "timestamp": _now(),
        })

        transaction_id = None
        if resp and resp[0] == 3:
            transaction_id = resp[2].get("transactionId")

        # Start MeterValues background task
        _active_sessions[connector_id] = {"meter_wh": 0, "transaction_id": transaction_id, "task": None}
        task = asyncio.ensure_future(_meter_values_loop(ws, connector_id, transaction_id))
        _active_sessions[connector_id]["task"] = task

    elif action == "RemoteStopTransaction":
        await ws.send(json.dumps([3, uid, {"status": "Accepted"}]))
        print("  <-- RemoteStopTransaction -> Accepted")

        # Find which connector is active
        connector_id = 1
        if len(_active_sessions) == 1:
            connector_id = next(iter(_active_sessions))

        session = _active_sessions.pop(connector_id, {})

        # Cancel MeterValues loop
        task = session.get("task")
        if task and not task.done():
            task.cancel()

        final_meter_wh = session.get("meter_wh", 0)
        transaction_id = session.get("transaction_id")

        await asyncio.sleep(1)
        await send_status(ws, connector_id, "Finishing")

        if transaction_id is not None:
            await send_call(ws, "StopTransaction", {
                "transactionId": transaction_id,
                "meterStop": final_meter_wh,
                "timestamp": _now(),
                "reason": "Remote",
            })

        await asyncio.sleep(2)
        await send_status(ws, connector_id, "Available")

    elif action == "Reset":
        reset_type = payload.get("type", "Hard")
        await ws.send(json.dumps([3, uid, {"status": "Accepted"}]))
        print("  <-- Reset ({}) -> Accepted (sim reconnects in 3s)".format(reset_type))
        # Simulate a reboot by closing the websocket — simulate() will reconnect
        await asyncio.sleep(3)
        await ws.close()

    else:
        await ws.send(json.dumps([4, uid, "NotImplemented",
                                  "Action '{}' not supported".format(action), {}]))
        print("  <-- {} -> NotImplemented".format(action))


# ---------------------------------------------------------------------------
# Receiver task: runs forever, routes all incoming messages
# ---------------------------------------------------------------------------

async def _receiver(ws):
    async for raw in ws:
        try:
            msg = json.loads(raw)
        except (ValueError, KeyError):
            continue
        if not isinstance(msg, list) or len(msg) < 2:
            continue

        msg_type = msg[0]
        uid = msg[1]

        if msg_type in (3, 4):
            # Response to one of our calls
            fut = _pending.pop(uid, None)
            if fut and not fut.done():
                fut.set_result(msg)

        elif msg_type == 2:
            # Server-initiated call
            action = msg[2] if len(msg) > 2 else ""
            payload = msg[3] if len(msg) > 3 else {}
            # Schedule handling without blocking the receiver loop
            asyncio.ensure_future(_handle_server_call(ws, uid, action, payload))


# ---------------------------------------------------------------------------
# Heartbeat task: sends Heartbeat every HEARTBEAT_INTERVAL seconds
# ---------------------------------------------------------------------------

async def _heartbeat_loop(ws):
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        await send_call(ws, "Heartbeat", {})


# ---------------------------------------------------------------------------
# Main simulation
# ---------------------------------------------------------------------------

async def simulate():
    print("[SIM] Connecting as '{}' to {} ...".format(CP_ID, URL))
    async with websockets.connect(URL, subprotocols=["ocpp1.6"]) as ws:
        print("[SIM] Connected. Subprotocol: {}\n".format(ws.subprotocol))

        recv_task = asyncio.ensure_future(_receiver(ws))

        try:
            await send_call(ws, "BootNotification", {
                "chargePointVendor": "Vestel",
                "chargePointModel": "VE7000",
                "chargePointSerialNumber": "SN-{}".format(CP_ID),
                "chargeBoxSerialNumber": "CB-{}".format(CP_ID),
                "firmwareVersion": "1.2.0",
                "meterType": "AC",
            })

            await send_status(ws, 0, "Available")
            await send_status(ws, 1, "Available")

            print("\n[SIM] Running (Ctrl+C to stop). Heartbeat every {}s.\n".format(HEARTBEAT_INTERVAL))

            await _heartbeat_loop(ws)
        finally:
            recv_task.cancel()
            # Cancel any active MeterValues tasks on disconnect
            for session in list(_active_sessions.values()):
                task = session.get("task")
                if task and not task.done():
                    task.cancel()
            _active_sessions.clear()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(simulate())
    except KeyboardInterrupt:
        print("\n[SIM] Stopped.")
