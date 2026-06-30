"""
SQLite kalıcı depolama katmanı.

Tablolar:
  transactions — OCPP işlem kayıtları (sunucu yeniden başlasa kaybolmaz)
  login_log    — giriş denemeleri (kullanıcı, IP, zaman, başarı)
  users        — admin tarafından yönetilen normal kullanıcı hesapları

Senkron sqlite3 çağrıları — tek asyncio event loop'ta milisaniye sürer, sorun yok.
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
    """Tabloları yoksa oluştur. Startup'ta bir kez çağrılır."""
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
    """)
    conn.commit()
    conn.close()
    print("  DB              : {}".format(_DB_PATH))


def load_transactions():
    """
    Tüm transaction kayıtlarını SQLite'tan yükle.
    Döndürür: {int tid: dict} — state.transactions formatında.
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
    print("  Yüklenen işlem  : {}".format(len(result)))
    return result


def upsert_transaction(tid: int, data: dict):
    """Bir transaction kaydını ekle veya güncelle (INSERT OR REPLACE)."""
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
    """Bir giriş denemesini logla."""
    now = datetime.datetime.utcnow().isoformat() + "Z"
    conn = _connect()
    conn.execute(
        "INSERT INTO login_log (username, ip, login_time, success) VALUES (?, ?, ?, ?)",
        (username, ip, now, 1 if success else 0),
    )
    conn.commit()
    conn.close()


def find_user(username: str):
    """SQLite users tablosunda kullanıcıyı bul. Bulunamazsa None döner."""
    conn = _connect()
    row = conn.execute(
        "SELECT username, password FROM users WHERE username = ?", (username,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_users():
    """Tüm normal kullanıcıları döndür (şifreler hariç)."""
    conn = _connect()
    rows = conn.execute(
        "SELECT username, created_at FROM users ORDER BY created_at DESC"
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def add_user(username: str, password: str) -> bool:
    """Yeni normal kullanıcı ekle. Kullanıcı zaten varsa False döner."""
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
    """Normal kullanıcıyı sil."""
    conn = _connect()
    conn.execute("DELETE FROM users WHERE username = ?", (username,))
    conn.commit()
    conn.close()


def get_login_log(limit: int = 200):
    """Son giriş kayıtlarını döndür (en yeni önce)."""
    conn = _connect()
    rows = conn.execute(
        "SELECT username, ip, login_time, success FROM login_log ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]
