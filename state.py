"""
Shared in-memory state for the CPMS.
Single asyncio event loop — no locks needed; coroutines are cooperative.
"""
import asyncio
import collections

# {cp_id: {vendor, model, serial, firmware, connectors, last_heartbeat, connected_at}}
chargers = {}

# Ring buffer per charge point: {cp_id: deque(maxlen=200)}
_LOG_PER_CP = 200
log_buffer: dict = {}

# One asyncio.Queue per open SSE connection
_sse_queues = []

# Active WebSocket connections: {cp_id: websocket}
ws_connections = {}

# Transaction records: {int tid: {cp_id, connector_id, id_tag, meter_start, meter_stop,
#                                  start_time, stop_time, energy_wh, reason}}
transactions = {}
_next_transaction_id = 1


def next_transaction_id():
    global _next_transaction_id
    tid = _next_transaction_id
    _next_transaction_id += 1
    return tid


def broadcast(event: dict):
    """
    Push an event to every SSE subscriber.
    Log-type events are also stored in log_buffer.
    """
    if event.get("type") == "log":
        cp_id = event.get("cp_id")
        if cp_id:
            if cp_id not in log_buffer:
                log_buffer[cp_id] = collections.deque(maxlen=_LOG_PER_CP)
            log_buffer[cp_id].append(event)
    dead = []
    for q in _sse_queues:
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        _sse_queues.remove(q)


def add_sse_client():
    """Register a new SSE client. Returns its dedicated asyncio.Queue."""
    q = asyncio.Queue(maxsize=200)
    _sse_queues.append(q)
    return q


def remove_sse_client(q):
    try:
        _sse_queues.remove(q)
    except ValueError:
        pass


def get_initial_snapshot():
    """
    Return (log_list, chargers_dict) snapshot for replaying history
    to a newly connected SSE client.
    """
    all_logs = []
    for cp_logs in log_buffer.values():
        all_logs.extend(cp_logs)
    all_logs.sort(key=lambda e: e.get("ts", ""))
    logs = all_logs
    cps = {}
    for cp_id, info in chargers.items():
        cps[cp_id] = {
            "vendor": info.get("vendor", ""),
            "model": info.get("model", ""),
            "serial": info.get("serial", ""),
            "firmware": info.get("firmware", ""),
            "last_heartbeat": info.get("last_heartbeat"),
            "connected_at": info.get("connected_at"),
            "connectors": {k: dict(v) for k, v in info.get("connectors", {}).items()},
        }
    return logs, cps
