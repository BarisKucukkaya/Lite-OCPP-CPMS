# lite-OCPP-CPMS

Lightweight Charge Point Management System (CPMS) implementing OCPP 1.6J over WebSocket, with a real-time web dashboard. Built with Python asyncio — no external frameworks.

## Features

- **OCPP 1.6J** WebSocket server — BootNotification, Heartbeat, StatusNotification, Authorize, StartTransaction, StopTransaction, MeterValues
- **Remote commands** — RemoteStartTransaction, RemoteStopTransaction, Reset (Hard/Soft)
- **Real-time dashboard** via Server-Sent Events (SSE) — no polling
- **Authentication** — session-based login/logout, 8-hour token TTL
- **Role-based access** — admin (full access) vs. user (read + remote actions only)
- **SQLite persistence** — transactions and login history survive restarts
- **Per-CP message log** — independent 200-entry ring buffer per charge point
- **Status filters** — filter stations by Available / Charging / Faulted / Offline
- **PowerLoss handling** — open transactions are auto-closed on charger disconnect
- **Multi-CP simulator** — simulate up to N charge points with auto-reconnect

## Requirements

- Python 3.6+
- Ubuntu 18.04+ (or any Linux with asyncio support)

```bash
pip3 install -r requirements.txt
```

## Setup

```bash
# 1. Clone the repo
git clone <repo-url>
cd lite-OCPP-CPMS

# 2. Create config from example and set your password
cp config.example.py config.py
# Edit config.py — change the admin password before running

# 3. Start the server
python3 main.py
```

Open the dashboard: **http://localhost:8000**

## Usage

### Simulator

```bash
# 10 charge points (CP001–CP010) on localhost
python3 sim_multi.py

# Custom server IP
python3 sim_multi.py 192.168.1.10

# 5 charge points (CP001–CP005)
python3 sim_multi.py localhost 5

# 10 charge points starting from CP003
python3 sim_multi.py localhost 10 3
```

Each simulated CP boots, sends StatusNotification, and waits for RemoteStartTransaction. On start it cycles `Preparing → Charging` and sends MeterValues every 5s. On Reset it reconnects automatically after 3s.

### Dashboard pages

| Page | Access | Description |
|------|--------|-------------|
| Genel Bakış | all | Status overview, live energy chart, recent transactions |
| İstasyonlar | all | CP cards with status filter chips; Start / Stop / Reset per CP |
| İşlemler | all | Transaction history with energy totals |
| Mesaj Logu | all | Per-CP OCPP message log |
| Giriş Logları | admin | Successful login history with username search |
| Kullanıcılar | admin | Add / remove normal users |

## Architecture

```
main.py
├── ocpp_server.py  (ws://0.0.0.0:9000/ocpp/<CP_ID>)
├── http_server.py  (http://0.0.0.0:8000)
├── state.py        ← shared in-memory bus (no locks, single event loop)
├── db.py           ← SQLite (cpms.db) — transactions, login_log, users
└── config.py       ← gitignored — admin credentials, session TTL
```

`state.py` is the shared memory bus. Both servers read/write it directly inside the same asyncio event loop, so no locks are needed (cooperative multitasking).

## Security notes

- `config.py` is gitignored — never commit credentials
- Passwords are stored in plain text (SQLite `users` table) — suitable for internal/lab use; add bcrypt before any public deployment
- Sessions use `HttpOnly; SameSite=Lax` cookies with configurable TTL
