"""
OCPP 1.6J Charge Point Simulator

Simulates a charge point that:
  1. Connects and sends BootNotification
  2. Sends StatusNotification (Available) for connectors 0 and 1
  3. Sends Heartbeat every 20 s (matching server interval)
  4. Listens for RemoteStartTransaction / RemoteStopTransaction from server
     and changes connector status accordingly

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
# Handle server-initiated calls (RemoteStart / RemoteStop)
# ---------------------------------------------------------------------------

async def _handle_server_call(ws, uid, action, payload):
    if action == "RemoteStartTransaction":
        connector_id = payload.get("connectorId", 1)
        await ws.send(json.dumps([3, uid, {"status": "Accepted"}]))
        print("  <-- RemoteStartTransaction (connector {}) -> Accepted".format(connector_id))
        # Simulate the charging sequence
        await asyncio.sleep(1)
        await send_status(ws, connector_id, "Preparing")
        await asyncio.sleep(2)
        await send_status(ws, connector_id, "Charging")

    elif action == "RemoteStopTransaction":
        await ws.send(json.dumps([3, uid, {"status": "Accepted"}]))
        print("  <-- RemoteStopTransaction -> Accepted")
        await asyncio.sleep(1)
        await send_status(ws, 1, "Finishing")
        await asyncio.sleep(2)
        await send_status(ws, 1, "Available")

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

        # Start receiver first so send_call futures can be resolved
        recv_task = asyncio.ensure_future(_receiver(ws))

        # Boot sequence
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

        # Heartbeat runs alongside receiver
        await _heartbeat_loop(ws)
        recv_task.cancel()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(simulate())
    except KeyboardInterrupt:
        print("\n[SIM] Stopped.")
