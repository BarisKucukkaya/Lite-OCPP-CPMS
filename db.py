"""
SQLite persistence layer.

Tables:
  transactions — OCPP transaction records (survive a server restart)
  login_log    — login attempts (username, IP, timestamp, success)
  users        — regular user accounts managed by an admin
  rfid_cards   — RFID cards authorized to start a charging session (idTag list)

Synchronous sqlite3 calls — each takes milliseconds on the single asyncio
event loop, which is fine.
"""
import sqlite3
import os
import datetime

_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cpms.db")


def _connect():
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init():
    """Create tables if they don't exist. Called once at startup."""
    conn = _connect()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id INTEGER PRIMARY KEY,
            cp_id          TEXT,
            connector_id   TEXT,
            id_tag         TEXT,
            meter_start    INTEGER DEFAULT 0,
            meter_stop     INTEGER,
            start_time     TEXT,
            stop_time      TEXT,
            energy_wh      INTEGER DEFAULT 0,
            reason         TEXT
        );

        CREATE TABLE IF NOT EXISTS login_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            username   TEXT,
            ip         TEXT,
            login_time TEXT,
            success    INTEGER
        );

        CREATE TABLE IF NOT EXISTS users (
            username   TEXT PRIMARY KEY,
            password   TEXT NOT NULL,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS rfid_cards (
            id_tag     TEXT PRIMARY KEY,
            label      TEXT,
            created_at TEXT
        );
    """)
    conn.commit()
    cols = [row["name"] for row in conn.execute("PRAGMA table_info(login_log)").fetchall()]
    if "logout_time" not in cols:
        conn.execute("ALTER TABLE login_log ADD COLUMN logout_time TEXT")
        conn.commit()
    conn.close()
    print("  DB              : {}".format(_DB_PATH))


def load_transactions():
    """
    Load all transaction records from SQLite.
    Returns: {int tid: dict} — in state.transactions format.
    """
    conn = _connect()
    rows = conn.execute("SELECT * FROM transactions ORDER BY transaction_id").fetchall()
    conn.close()
    result = {}
    for row in rows:
        tid = row["transaction_id"]
        result[tid] = {
            "cp_id":        row["cp_id"],
            "connector_id": row["connector_id"],
            "id_tag":       row["id_tag"],
            "meter_start":  row["meter_start"],
            "meter_stop":   row["meter_stop"],
            "start_time":   row["start_time"],
            "stop_time":    row["stop_time"],
            "energy_wh":    row["energy_wh"] or 0,
            "reason":       row["reason"],
        }
    print("  Loaded txns     : {}".format(len(result)))
    return result


def upsert_transaction(tid: int, data: dict):
    """Insert or update a transaction record (INSERT OR REPLACE)."""
    conn = _connect()
    conn.execute(
        """INSERT OR REPLACE INTO transactions
           (transaction_id, cp_id, connector_id, id_tag,
            meter_start, meter_stop, start_time, stop_time, energy_wh, reason)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            tid,
            data.get("cp_id"),
            data.get("connector_id"),
            data.get("id_tag"),
            data.get("meter_start", 0),
            data.get("meter_stop"),
            data.get("start_time"),
            data.get("stop_time"),
            data.get("energy_wh", 0),
            data.get("reason"),
        ),
    )
    conn.commit()
    conn.close()


def log_login(username: str, ip: str, success: bool):
    """Log a login attempt. Returns the id of the inserted row."""
    now = datetime.datetime.utcnow().isoformat() + "Z"
    conn = _connect()
    cur = conn.execute(
        "INSERT INTO login_log (username, ip, login_time, success) VALUES (?, ?, ?, ?)",
        (username, ip, now, 1 if success else 0),
    )
    conn.commit()
    conn.close()
    return cur.lastrowid


def mark_logout(login_log_id: int):
    """Record the logout time on a login entry."""
    now = datetime.datetime.utcnow().isoformat() + "Z"
    conn = _connect()
    conn.execute("UPDATE login_log SET logout_time = ? WHERE id = ?", (now, login_log_id))
    conn.commit()
    conn.close()


def find_user(username: str):
    """Look up a user in the SQLite users table. Returns None if not found."""
    conn = _connect()
    row = conn.execute(
        "SELECT username, password FROM users WHERE username = ?", (username,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_users():
    """Return all regular users (excluding passwords)."""
    conn = _connect()
    rows = conn.execute(
        "SELECT username, created_at FROM users ORDER BY created_at DESC"
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def add_user(username: str, password: str) -> bool:
    """Add a new regular user. Returns False if the user already exists."""
    now = datetime.datetime.utcnow().isoformat() + "Z"
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO users (username, password, created_at) VALUES (?, ?, ?)",
            (username, password, now),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def delete_user(username: str):
    """Delete a regular user."""
    conn = _connect()
    conn.execute("DELETE FROM users WHERE username = ?", (username,))
    conn.commit()
    conn.close()


def get_cards():
    """Return all authorized RFID cards."""
    conn = _connect()
    rows = conn.execute(
        "SELECT id_tag, label, created_at FROM rfid_cards ORDER BY created_at DESC"
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def add_card(id_tag: str, label: str) -> bool:
    """Add a new authorized RFID card. Returns False if it already exists."""
    now = datetime.datetime.utcnow().isoformat() + "Z"
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO rfid_cards (id_tag, label, created_at) VALUES (?, ?, ?)",
            (id_tag, label, now),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def delete_card(id_tag: str):
    """Delete an authorized RFID card."""
    conn = _connect()
    conn.execute("DELETE FROM rfid_cards WHERE id_tag = ?", (id_tag,))
    conn.commit()
    conn.close()


def is_card_authorized(id_tag: str) -> bool:
    """Is this idTag registered in the rfid_cards table?"""
    conn = _connect()
    row = conn.execute(
        "SELECT 1 FROM rfid_cards WHERE id_tag = ?", (id_tag,)
    ).fetchone()
    conn.close()
    return row is not None


def get_login_log(limit: int = 200, username_filter: str = ""):
    """Return successful login records (newest first). username_filter does a LIKE search."""
    conn = _connect()
    if username_filter:
        rows = conn.execute(
            "SELECT username, ip, login_time, logout_time FROM login_log "
            "WHERE success = 1 AND username LIKE ? ORDER BY id DESC LIMIT ?",
            ("%" + username_filter + "%", limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT username, ip, login_time, logout_time FROM login_log "
            "WHERE success = 1 ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    conn.close()
    return [dict(row) for row in rows]
