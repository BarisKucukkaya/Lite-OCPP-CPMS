# lite-OCPP-CPMS

A lightweight Charge Point Management System (CPMS) implementing OCPP 1.6 over WebSocket, with a real-time web dashboard.

## Features

- OCPP 1.6 WebSocket server for charge point communication
- Real-time dashboard with Server-Sent Events (SSE)
- Remote Start/Stop transaction support
- Charge point simulator for testing

## Requirements

- Python 3.6+
- Ubuntu 18.04+

```bash
pip3 install -r requirements.txt
```

## Usage

**Start the server** (OCPP on port 9000, dashboard on port 8000):
```bash
python3 main.py
```

**Run a simulated charge point:**
```bash
python3 sim.py CP001
python3 sim.py CP002 192.168.1.10   # custom ID + server IP
```

**Open dashboard:**
```
http://localhost:8000
```

## Architecture

Two async servers run in a single asyncio event loop:

| Component | File | Port |
|-----------|------|------|
| OCPP WebSocket server | `ocpp_server.py` | 9000 |
| HTTP dashboard | `http_server.py` | 8000 |

`state.py` acts as the shared in-memory state between both servers, using asyncio cooperative multitasking (no locks needed).
