# lite-OCPP-CPMS

Lightweight Charge Point Management System (CPMS) implementing OCPP 1.6J over WebSocket, with a
real-time web dashboard. Python asyncio backend (no framework) — vanilla HTML/CSS/JS frontend
(Chart.js via CDN for charts). Dashboard UI text is in Turkish.

## Features

- **OCPP 1.6J WebSocket server** — BootNotification, Heartbeat, StatusNotification, Authorize,
  StartTransaction, StopTransaction, MeterValues (CP→Server); RemoteStartTransaction (requires
  idTag), RemoteStopTransaction, Reset (Hard/Soft) (Server→CP)
- **Spec-compliant validation** — invalid `StatusNotification.status`/`errorCode` and
  `StopTransaction.reason` values are rejected with a proper `PropertyConstraintViolation`
  CallError instead of being silently accepted; unexpected handler errors return an
  `InternalError` CallError instead of dropping the connection
- **SoC tracking & auto-stop** — reads `SoC` (State of Charge / battery %) from
  MeterValues/StopTransaction, shows it live on the dashboard, and automatically sends
  RemoteStopTransaction once the EV reaches 100%
- **Real-time dashboard** via Server-Sent Events (SSE) — no polling; toast notifications for
  connect / disconnect / fault / transaction-complete / auto-stop events
- **Live charts** — energy flow (line) and connector status distribution (donut)
- **Authentication** — session-based login/logout (`HttpOnly` cookie, 8-hour TTL by default)
- **Role-based access** — admin (full access incl. user management & login logs) vs. user
  (stations, transactions, message log — can still Start/Stop/Reset)
- **SQLite persistence** — transactions, login history and managed user accounts survive
  restarts (live charger/connector state does not — chargers simply re-register on reconnect)
- **Per-CP message log** — independent 200-entry ring buffer per charge point, expandable
  JSON payloads (MeterValues excluded to reduce noise)
- **Status filters** — filter stations by Available / Charging / Faulted / Offline
- **PowerLoss handling** — open transactions auto-close with `reason: PowerLoss` if a charger
  disconnects mid-session
- **Multi-CP simulator** — simulate up to N charge points with auto-reconnect; optional `--soc`
  flag simulates a climbing battery percentage to exercise the auto-stop feature

## Requirements

- Python 3.6+ (developed on 3.6.9 / Ubuntu 18.04)
- One pip dependency: `websockets==9.1`
- Internet access for the browser loading the dashboard (Chart.js is loaded from
  `cdn.jsdelivr.net`; without it the rest of the dashboard still works, only the two charts
  won't render)

```bash
pip3 install -r requirements.txt
```

## Setup

```bash
# 1. Clone the repo
git clone <repo-url>
cd lite-OCPP-CPMS

# 2. Create config.py (gitignored — no template ships in the repo)
cat > config.py <<'EOF'
ADMIN_USERS = {"admin": "change-me"}
SESSION_TTL = 8 * 3600  # session cookie lifetime, seconds
EOF
# Edit config.py and set a real admin password before running

# 3. Start the server
python3 main.py
```

Open the dashboard: **http://localhost:8000** (log in with the admin credentials from `config.py`)

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

# Simulate battery % climbing to 100% over ~5 minutes (triggers server-side auto-stop)
python3 sim_multi.py localhost 1 --soc
```

Each simulated CP boots, sends StatusNotification, and waits for a `RemoteStartTransaction` — the
dashboard's Start button prompts for an `idTag` before sending one (required by the OCPP 1.6
schema). On start it cycles `Preparing → Charging`, sends `StartTransaction`, and reports
`MeterValues` every 5s (energy + power, plus a climbing SoC sample with `--soc`). Once SoC
reaches 100%, the server automatically issues `RemoteStopTransaction` — no manual action needed.
On `Reset` the simulated CP disconnects and reconnects automatically after 3s.

### Dashboard pages

| Page | Access | Description |
|------|--------|-------------|
| Overview (*Genel Bakış*) | all | Status overview, clickable stat filters, live energy chart, connector-status donut chart, recent transactions |
| Stations (*İstasyonlar*) | all | CP cards with status filter chips; Start (prompts for idTag) / Stop / Reset / view-log per CP; live kWh, power, SoC%, and session-duration badges |
| Transactions (*İşlemler*) | all | Full transaction history with energy totals and stop reason |
| Message Log (*Mesaj Logu*) | all | Per-CP raw OCPP message log (MeterValues excluded), expandable payloads |
| Login Logs (*Giriş Logları*) | admin | Successful login history with username search |
| Users (*Kullanıcılar*) | admin | Add / remove regular user accounts |

## Architecture

```
main.py
├── ocpp_server.py  (ws://0.0.0.0:9000/ocpp/<CP_ID>) — OCPP 1.6J handling, transactions, SoC auto-stop
├── http_server.py  (http://0.0.0.0:8000)            — hand-rolled async HTTP + SSE, no framework
├── state.py        ← shared in-memory bus (no locks, single event loop)
├── db.py           ← SQLite (cpms.db) — transactions, login_log, users
├── sim_multi.py    ← multi-CP OCPP simulator for testing
└── config.py       ← gitignored, no template — admin credentials & session TTL (see Setup)
```

`state.py` is the shared memory bus. Both servers read/write it directly inside the same asyncio
event loop, so no locks are needed (cooperative multitasking). Live charger/connector state lives
only in memory; transaction history, login log and user accounts persist to SQLite (`cpms.db`,
gitignored) and survive a restart.

## Security notes

- `config.py` is gitignored with no committed template — create it yourself (see Setup) and
  never commit real credentials
- Passwords (both `config.py` admins and SQLite `users`) are stored in plain text — suitable for
  internal/lab use only; add hashing (e.g. bcrypt) before any public deployment
- Sessions use `HttpOnly; SameSite=Lax` cookies with a configurable TTL (`config.SESSION_TTL`)
- No automated test suite or CI pipeline exists yet
