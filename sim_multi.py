"""
OCPP 1.6J Multi-CP Simulator

Her CP için bağımsız asyncio task çalıştırır. Tüm state (pending, active_sessions)
fonksiyon-lokal değişkenlerde tutulur — paylaşım yok.

Kullanım:
    python3 sim_multi.py                       # localhost, 10 CP (CP001-CP010)
    python3 sim_multi.py 192.168.1.10          # özel sunucu IP
    python3 sim_multi.py localhost 5           # 5 CP (CP001-CP005)
    python3 sim_multi.py localhost 10 1        # 10 CP, CP001'den başla
"""
import asyncio
import json
import sys
import uuid
import datetime
import websockets

SIMULATE_SOC = "--soc" in sys.argv
_args = [a for a in sys.argv[1:] if a != "--soc"]

HOST  = _args[0] if len(_args) > 0 else "localhost"
COUNT = int(_args[1]) if len(_args) > 1 else 10
START = int(_args[2]) if len(_args) > 2 else 1

PORT               = 9000
HEARTBEAT_INTERVAL = 20
SOC_RAMP_SECONDS    = 300  # --soc ile %0'dan %100'e çıkış süresi (~5 dakika)


def _now():
    return datetime.datetime.utcnow().isoformat() + "Z"


async def run_cp(cp_id):
    """Tek bir şarj noktasını simüle eder. Bağlantı kopunca yeniden bağlanır."""
    url     = "ws://{}:{}/ocpp/{}".format(HOST, PORT, cp_id)
    pending = {}          # uid → Future
    sessions = {}         # connector_id → {meter_wh, transaction_id, task}

    def _uid():
        return str(uuid.uuid4())[:8]

    async def send_call(ws, action, payload):
        uid = _uid()
        fut = asyncio.get_event_loop().create_future()
        pending[uid] = fut
        await ws.send(json.dumps([2, uid, action, payload]))
        print("  [{}] --> {}".format(cp_id, action))
        try:
            msg = await asyncio.wait_for(fut, timeout=10)
        except asyncio.TimeoutError:
            pending.pop(uid, None)
            print("  [{}] !!! Timeout: {}".format(cp_id, action))
            return None
        if msg[0] == 3:
            print("  [{}] <-- {}Response".format(cp_id, action))
        elif msg[0] == 4:
            print("  [{}] <-- ERROR: {}".format(cp_id, msg[3]))
        return msg

    async def send_status(ws, connector_id, status):
        ec = "GroundFailure" if status == "Faulted" else "NoError"
        await send_call(ws, "StatusNotification", {
            "connectorId": connector_id,
            "errorCode": ec,
            "status": status,
            "timestamp": _now(),
        })

    async def meter_loop(ws, connector_id, transaction_id):
        session = sessions.get(connector_id, {})
        while True:
            await asyncio.sleep(5)
            session["meter_wh"] = session.get("meter_wh", 0) + 55
            kwh = round(session["meter_wh"] / 1000.0, 4)
            sampled_values = [
                {
                    "value": str(kwh),
                    "measurand": "Energy.Active.Import.Register",
                    "unit": "kWh",
                    "context": "Sample.Periodic",
                    "format": "Raw",
                    "location": "Outlet",
                },
                {
                    "value": "22000",
                    "measurand": "Power.Active.Import",
                    "unit": "W",
                    "context": "Sample.Periodic",
                    "format": "Raw",
                    "location": "Outlet",
                },
            ]
            if SIMULATE_SOC:
                soc_increment = 100.0 * 5 / SOC_RAMP_SECONDS
                session["soc"] = min(session.get("soc", 0) + soc_increment, 100)
                sampled_values.append({
                    "value": str(int(round(session["soc"]))),
                    "measurand": "SoC",
                    "unit": "Percent",
                    "context": "Sample.Periodic",
                    "format": "Raw",
                    "location": "EV",
                })
            await send_call(ws, "MeterValues", {
                "connectorId": connector_id,
                "transactionId": transaction_id,
                "meterValue": [{
                    "timestamp": _now(),
                    "sampledValue": sampled_values,
                }],
            })

    async def handle_server_call(ws, uid, action, payload):
        if action == "RemoteStartTransaction":
            connector_id = payload.get("connectorId", 1)
            await ws.send(json.dumps([3, uid, {"status": "Accepted"}]))
            print("  [{}] <-- RemoteStartTransaction → Accepted".format(cp_id))
            await asyncio.sleep(1)
            await send_status(ws, connector_id, "Preparing")
            await asyncio.sleep(2)
            await send_status(ws, connector_id, "Charging")
            resp = await send_call(ws, "StartTransaction", {
                "connectorId": connector_id,
                "idTag": "SIM001",
                "meterStart": 0,
                "timestamp": _now(),
            })
            tid = None
            if resp and resp[0] == 3:
                tid = resp[2].get("transactionId")
            sessions[connector_id] = {"meter_wh": 0, "transaction_id": tid, "task": None}
            task = asyncio.ensure_future(meter_loop(ws, connector_id, tid))
            sessions[connector_id]["task"] = task

        elif action == "RemoteStopTransaction":
            await ws.send(json.dumps([3, uid, {"status": "Accepted"}]))
            print("  [{}] <-- RemoteStopTransaction → Accepted".format(cp_id))
            connector_id = next(iter(sessions), 1)
            session = sessions.pop(connector_id, {})
            t = session.get("task")
            if t and not t.done():
                t.cancel()
            final_wh = session.get("meter_wh", 0)
            tid = session.get("transaction_id")
            await asyncio.sleep(1)
            await send_status(ws, connector_id, "Finishing")
            if tid is not None:
                await send_call(ws, "StopTransaction", {
                    "transactionId": tid,
                    "meterStop": final_wh,
                    "timestamp": _now(),
                    "reason": "Remote",
                })
            await asyncio.sleep(2)
            await send_status(ws, connector_id, "Available")

        elif action == "Reset":
            reset_type = payload.get("type", "Hard")
            await ws.send(json.dumps([3, uid, {"status": "Accepted"}]))
            print("  [{}] <-- Reset ({}) → yeniden bağlanıyor...".format(cp_id, reset_type))
            await asyncio.sleep(3)
            await ws.close()

        else:
            await ws.send(json.dumps([4, uid, "NotImplemented",
                                      "Action '{}' not supported".format(action), {}]))

    async def receiver(ws):
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except (ValueError, KeyError):
                continue
            if not isinstance(msg, list) or len(msg) < 2:
                continue
            msg_type, uid = msg[0], msg[1]
            if msg_type in (3, 4):
                fut = pending.pop(uid, None)
                if fut and not fut.done():
                    fut.set_result(msg)
            elif msg_type == 2:
                action  = msg[2] if len(msg) > 2 else ""
                payload = msg[3] if len(msg) > 3 else {}
                asyncio.ensure_future(handle_server_call(ws, uid, action, payload))

    async def heartbeat_loop(ws):
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            await send_call(ws, "Heartbeat", {})

    # Bağlantı döngüsü — kesilince yeniden bağlanır
    while True:
        try:
            print("[SIM] {} bağlanıyor...".format(cp_id))
            async with websockets.connect(url, subprotocols=["ocpp1.6"]) as ws:
                recv_task = asyncio.ensure_future(receiver(ws))
                try:
                    await send_call(ws, "BootNotification", {
                        "chargePointVendor": "Vestel",
                        "chargePointModel": "VE7000",
                        "chargePointSerialNumber": "SN-{}".format(cp_id),
                        "chargeBoxSerialNumber": "CB-{}".format(cp_id),
                        "firmwareVersion": "1.2.0",
                        "meterType": "AC",
                    })
                    await send_status(ws, 0, "Available")
                    await send_status(ws, 1, "Available")
                    print("[SIM] {} hazır.\n".format(cp_id))
                    await heartbeat_loop(ws)
                finally:
                    recv_task.cancel()
                    for s in list(sessions.values()):
                        t = s.get("task")
                        if t and not t.done():
                            t.cancel()
                    sessions.clear()
                    pending.clear()
        except Exception as e:
            print("[SIM] {} hata: {} — 5s sonra yeniden deneniyor...".format(cp_id, e))
            await asyncio.sleep(5)


async def main():
    cp_ids = ["CP{:03d}".format(START + i) for i in range(COUNT)]
    print("[SIM] {} istasyon başlatılıyor: {} → {}".format(COUNT, cp_ids[0], cp_ids[-1]))
    print("[SIM] Sunucu: {}:{}\n".format(HOST, PORT))

    tasks = []
    for i, cp_id in enumerate(cp_ids):
        tasks.append(asyncio.ensure_future(run_cp(cp_id)))
        # Kademeli bağlantı — sunucuyu ezmemek için
        await asyncio.sleep(0.4)

    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\n[SIM] Durduruldu.")
