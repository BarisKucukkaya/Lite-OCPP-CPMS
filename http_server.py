"""
Async HTTP server for the dashboard (asyncio.start_server — zero extra deps).

Endpoints (auth gerektirmeyenler):
  GET  /login              -> static/login.html
  POST /api/login          -> {username, password} -> Set-Cookie session
  POST /api/logout         -> cookie sil, 302 /login

Endpoints (session cookie gerektirir):
  GET  /                   -> static/index.html
  GET  /api/chargers       -> current charger state as JSON
  GET  /api/transactions   -> last 50 transactions as JSON
  GET  /api/login-log      -> giriş logları as JSON
  GET  /events             -> SSE stream (text/event-stream)
  POST /api/action         -> send RemoteStart/Stop/Reset to a charge point
"""
import asyncio
import json
import os
import datetime
import uuid
import state
import ocpp_server
import config
import db

_STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

_CORS_HEADERS = (
    b"Access-Control-Allow-Origin: *\r\n"
    b"Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
    b"Access-Control-Allow-Headers: Content-Type\r\n"
)

# {token_str: {username, created_at, ip}}
_sessions = {}


# ---------------------------------------------------------------------------
# Session yönetimi
# ---------------------------------------------------------------------------

def _parse_session_token(headers: dict):
    """Cookie header'ından 'session' değerini çıkar."""
    cookie = headers.get("cookie", "")
    for part in cookie.split(";"):
        k, _, v = part.strip().partition("=")
        if k.strip() == "session":
            return v.strip()
    return None


def _get_session(headers: dict):
    """Geçerli session varsa döndür, yoksa None."""
    token = _parse_session_token(headers)
    if not token:
        return None
    sess = _sessions.get(token)
    if not sess:
        return None
    age = (datetime.datetime.utcnow() - sess["created_at"]).total_seconds()
    if age > config.SESSION_TTL:
        _sessions.pop(token, None)
        return None
    # Geriye dönük uyumluluk: eski session'larda role yoksa türet
    if "role" not in sess:
        sess["role"] = "admin" if sess.get("username") in config.ADMIN_USERS else "user"
    return sess


async def _require_auth(headers: dict, writer) -> bool:
    """
    Auth kontrolü yapar.
    Oturum geçerliyse False döner (devam et).
    Geçersizse 302 /login yazar ve True döner (işlemi durdur).
    """
    if _get_session(headers) is not None:
        return False
    writer.write(
        b"HTTP/1.1 302 Found\r\n"
        b"Location: /login\r\n"
        b"Content-Length: 0\r\n"
        + _CORS_HEADERS
        + b"\r\n"
    )
    await writer.drain()
    return True


async def _require_admin(headers: dict, writer) -> bool:
    """
    Admin yetkisi gerektirir.
    Oturum yoksa 302 /login, yetersiz rol varsa 403 döner.
    True → işlemi durdur.  False → devam et.
    """
    sess = _get_session(headers)
    if sess is None:
        writer.write(
            b"HTTP/1.1 302 Found\r\n"
            b"Location: /login\r\n"
            b"Content-Length: 0\r\n"
            + _CORS_HEADERS
            + b"\r\n"
        )
        await writer.drain()
        return True
    if sess.get("role") != "admin":
        await _write_json(writer, {"error": "Yetersiz yetki"}, 403)
        return True
    return False


def _get_client_ip(headers: dict) -> str:
    return (
        headers.get("x-forwarded-for", "")
        or headers.get("x-real-ip", "")
        or "unknown"
    ).split(",")[0].strip()


# ---------------------------------------------------------------------------
# Low-level write helpers
# ---------------------------------------------------------------------------

async def _write_response(writer, status: int, extra_headers: bytes, body: bytes,
                          content_type: bytes = b"text/plain"):
    status_line = "HTTP/1.1 {} {}\r\n".format(status, _reason(status)).encode()
    headers = (
        status_line
        + b"Content-Type: " + content_type + b"\r\n"
        + b"Content-Length: " + str(len(body)).encode() + b"\r\n"
        + _CORS_HEADERS
        + extra_headers
        + b"\r\n"
    )
    writer.write(headers + body)
    await writer.drain()


async def _write_json(writer, obj, status: int = 200):
    body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    await _write_response(writer, status, b"", body,
                          content_type=b"application/json; charset=utf-8")


async def _write_sse_event(writer, event: dict):
    data = json.dumps(event, ensure_ascii=False)
    writer.write(("data: " + data + "\n\n").encode("utf-8"))
    await writer.drain()


def _reason(code: int) -> str:
    return {200: "OK", 204: "No Content", 302: "Found", 400: "Bad Request",
            401: "Unauthorized", 403: "Forbidden", 404: "Not Found",
            502: "Bad Gateway", 503: "Service Unavailable"}.get(code, "")


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

async def _serve_static(writer, filename: str):
    filepath = os.path.join(_STATIC_DIR, filename)
    try:
        with open(filepath, "rb") as f:
            body = f.read()
    except FileNotFoundError:
        await _write_response(writer, 404, b"", b"Not found")
        return
    await _write_response(writer, 200, b"", body,
                          content_type=b"text/html; charset=utf-8")


async def _serve_chargers(writer):
    _, cps = state.get_initial_snapshot()
    await _write_json(writer, cps)


async def _serve_sse(writer):
    writer.write(
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: text/event-stream; charset=utf-8\r\n"
        b"Cache-Control: no-cache\r\n"
        b"Connection: keep-alive\r\n"
        b"X-Accel-Buffering: no\r\n"
        + _CORS_HEADERS
        + b"\r\n"
    )
    await writer.drain()

    q = state.add_sse_client()
    try:
        logs, cps = state.get_initial_snapshot()
        for event in logs:
            await _write_sse_event(writer, event)
        for cp_id, info in cps.items():
            await _write_sse_event(writer, {"type": "charger_update", "cp_id": cp_id, **info})

        while True:
            try:
                event = await asyncio.wait_for(q.get(), timeout=15)
                await _write_sse_event(writer, event)
            except asyncio.TimeoutError:
                writer.write(b": keepalive\n\n")
                await writer.drain()
    except (BrokenPipeError, ConnectionResetError):
        pass
    finally:
        state.remove_sse_client(q)


async def _serve_transactions(writer):
    txns = sorted(
        [dict(v, transaction_id=k) for k, v in state.transactions.items()],
        key=lambda t: t.get("start_time", ""),
        reverse=True,
    )
    await _write_json(writer, txns[:50])


async def _handle_action(writer, body_bytes: bytes):
    try:
        data = json.loads(body_bytes)
    except (ValueError, TypeError):
        await _write_json(writer, {"error": "Invalid JSON body"}, 400)
        return

    cp_id = data.get("cp_id", "")
    action = data.get("action", "")
    connector_id = int(data.get("connector_id", 1))

    allowed = ("RemoteStartTransaction", "RemoteStopTransaction", "Reset")
    if action not in allowed:
        await _write_json(writer, {"error": "Unknown action"}, 400)
        return

    extra = {}
    if action == "Reset":
        reset_type = (data.get("reset_type") or "Hard").strip()
        if reset_type not in ocpp_server.VALID_RESET_TYPES:
            await _write_json(writer, {"error": "reset_type must be 'Hard' or 'Soft'"}, 400)
            return
        extra["reset_type"] = reset_type
    elif action == "RemoteStartTransaction":
        id_tag = (data.get("id_tag") or "").strip()
        if not id_tag:
            await _write_json(writer, {"error": "id_tag is required"}, 400)
            return
        if not db.is_card_authorized(id_tag):
            await _write_json(writer, {"error": "Bu RFID kartı sistemde kayıtlı değil"}, 400)
            return
        extra["id_tag"] = id_tag

    try:
        result = await ocpp_server.remote_action(cp_id, action, connector_id, extra=extra)
        await _write_json(writer, {"status": "ok", "result": result})
    except Exception as e:
        await _write_json(writer, {"error": str(e)}, 502)


async def _handle_login(writer, body_bytes: bytes, ip: str):
    try:
        data = json.loads(body_bytes)
    except (ValueError, TypeError):
        await _write_json(writer, {"error": "Invalid JSON"}, 400)
        return

    username = data.get("username", "")
    password = data.get("password", "")

    # Rol tespiti: önce config admins, sonra SQLite users
    role = None
    if config.ADMIN_USERS.get(username) == password:
        role = "admin"
    else:
        user_row = db.find_user(username)
        if user_row and user_row.get("password") == password:
            role = "user"

    if role is not None:
        token = str(uuid.uuid4())
        _sessions[token] = {
            "username": username,
            "role": role,
            "created_at": datetime.datetime.utcnow(),
            "ip": ip,
        }
        db.log_login(username, ip, success=True)
        cookie = "session={}; HttpOnly; Path=/; SameSite=Lax".format(token)
        body = json.dumps({"ok": True}, ensure_ascii=False).encode()
        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: application/json; charset=utf-8\r\n"
            + b"Content-Length: " + str(len(body)).encode() + b"\r\n"
            + b"Set-Cookie: " + cookie.encode() + b"\r\n"
            + _CORS_HEADERS
            + b"\r\n"
            + body
        )
        await writer.drain()
    else:
        db.log_login(username, ip, success=False)
        await _write_json(writer, {"error": "Kullanıcı adı veya şifre hatalı"}, 401)


async def _handle_logout(writer, headers: dict):
    token = _parse_session_token(headers)
    if token:
        _sessions.pop(token, None)
    # Cookie'yi sil (expires geçmişe)
    writer.write(
        b"HTTP/1.1 302 Found\r\n"
        b"Location: /login\r\n"
        b"Set-Cookie: session=; HttpOnly; Path=/; Max-Age=0\r\n"
        b"Content-Length: 0\r\n"
        + _CORS_HEADERS
        + b"\r\n"
    )
    await writer.drain()


# ---------------------------------------------------------------------------
# Request parser + router
# ---------------------------------------------------------------------------

async def _handle_client(reader, writer):
    try:
        request_line = await asyncio.wait_for(reader.readline(), timeout=10)
        if not request_line:
            return
        parts = request_line.decode("utf-8", errors="replace").strip().split(" ")
        if len(parts) < 2:
            return
        method, path = parts[0], parts[1]
        path = path.split("?")[0]

        headers = {}
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=10)
            if line in (b"\r\n", b"\n", b""):
                break
            if b":" in line:
                key, _, value = line.decode("utf-8", errors="replace").partition(":")
                headers[key.strip().lower()] = value.strip()

        if method == "OPTIONS":
            writer.write(b"HTTP/1.1 204 No Content\r\n" + _CORS_HEADERS + b"\r\n")
            await writer.drain()

        # ── Auth gerektirmeyen route'lar ────────────────────────────
        elif method == "GET" and path == "/login":
            await _serve_static(writer, "login.html")

        elif method == "POST" and path == "/api/login":
            length = int(headers.get("content-length", 0))
            body = await asyncio.wait_for(reader.read(length), timeout=10)
            ip = _get_client_ip(headers)
            await _handle_login(writer, body, ip)
            return  # _handle_login kendi response'unu yazar

        elif method == "POST" and path == "/api/logout":
            await _handle_logout(writer, headers)
            return

        # ── Korumalı route'lar ───────────────────────────────────────
        elif method == "GET" and path in ("/", "/index.html"):
            if await _require_auth(headers, writer):
                return
            await _serve_static(writer, "index.html")

        elif method == "GET" and path == "/api/chargers":
            if await _require_auth(headers, writer):
                return
            await _serve_chargers(writer)

        elif method == "GET" and path == "/api/transactions":
            if await _require_auth(headers, writer):
                return
            await _serve_transactions(writer)

        elif method == "GET" and path == "/api/me":
            if await _require_auth(headers, writer):
                return
            sess = _get_session(headers)
            await _write_json(writer, {"username": sess["username"], "role": sess["role"]})

        elif method == "GET" and path == "/api/login-log":
            if await _require_admin(headers, writer):
                return
            raw_qs = parts[1].split("?", 1)[1] if "?" in parts[1] else ""
            q_filter = ""
            for param in raw_qs.split("&"):
                if param.startswith("q="):
                    q_filter = param[2:]
                    break
            await _write_json(writer, db.get_login_log(username_filter=q_filter))

        elif method == "GET" and path == "/api/users":
            if await _require_admin(headers, writer):
                return
            await _write_json(writer, db.get_users())

        elif method == "POST" and path == "/api/users":
            if await _require_admin(headers, writer):
                return
            length = int(headers.get("content-length", 0))
            body_bytes = await asyncio.wait_for(reader.read(length), timeout=10)
            try:
                udata = json.loads(body_bytes)
            except (ValueError, TypeError):
                await _write_json(writer, {"error": "Invalid JSON"}, 400)
                return
            uname = udata.get("username", "").strip()
            upass = udata.get("password", "")
            if not uname or not upass:
                await _write_json(writer, {"error": "Kullanıcı adı ve şifre zorunlu"}, 400)
                return
            if uname in config.ADMIN_USERS:
                await _write_json(writer, {"error": "Bu kullanıcı adı ayrılmış"}, 400)
                return
            ok = db.add_user(uname, upass)
            if ok:
                await _write_json(writer, {"ok": True})
            else:
                await _write_json(writer, {"error": "Bu kullanıcı adı zaten mevcut"}, 400)

        elif method == "DELETE" and path == "/api/users":
            if await _require_admin(headers, writer):
                return
            length = int(headers.get("content-length", 0))
            body_bytes = await asyncio.wait_for(reader.read(length), timeout=10)
            try:
                udata = json.loads(body_bytes)
            except (ValueError, TypeError):
                await _write_json(writer, {"error": "Invalid JSON"}, 400)
                return
            uname = udata.get("username", "")
            if uname in config.ADMIN_USERS:
                await _write_json(writer, {"error": "Admin hesabı silinemez"}, 400)
                return
            db.delete_user(uname)
            await _write_json(writer, {"ok": True})

        elif method == "GET" and path == "/api/cards":
            if await _require_admin(headers, writer):
                return
            await _write_json(writer, db.get_cards())

        elif method == "POST" and path == "/api/cards":
            if await _require_admin(headers, writer):
                return
            length = int(headers.get("content-length", 0))
            body_bytes = await asyncio.wait_for(reader.read(length), timeout=10)
            try:
                cdata = json.loads(body_bytes)
            except (ValueError, TypeError):
                await _write_json(writer, {"error": "Invalid JSON"}, 400)
                return
            id_tag = (cdata.get("id_tag") or "").strip()
            label = (cdata.get("label") or "").strip()
            if not id_tag:
                await _write_json(writer, {"error": "idTag zorunlu"}, 400)
                return
            ok = db.add_card(id_tag, label)
            if ok:
                await _write_json(writer, {"ok": True})
            else:
                await _write_json(writer, {"error": "Bu kart zaten kayıtlı"}, 400)

        elif method == "DELETE" and path == "/api/cards":
            if await _require_admin(headers, writer):
                return
            length = int(headers.get("content-length", 0))
            body_bytes = await asyncio.wait_for(reader.read(length), timeout=10)
            try:
                cdata = json.loads(body_bytes)
            except (ValueError, TypeError):
                await _write_json(writer, {"error": "Invalid JSON"}, 400)
                return
            id_tag = cdata.get("id_tag", "")
            db.delete_card(id_tag)
            await _write_json(writer, {"ok": True})

        elif method == "GET" and path == "/events":
            if await _require_auth(headers, writer):
                return
            await _serve_sse(writer)
            return

        elif method == "POST" and path == "/api/action":
            if await _require_auth(headers, writer):
                return
            length = int(headers.get("content-length", 0))
            body = await asyncio.wait_for(reader.read(length), timeout=10)
            await _handle_action(writer, body)

        else:
            await _write_response(writer, 404, b"", b"Not Found")

    except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError):
        pass
    except Exception:
        pass
    finally:
        writer.close()


# ---------------------------------------------------------------------------
# Server startup
# ---------------------------------------------------------------------------

async def run_server(host: str = "0.0.0.0", port: int = 8000):
    await asyncio.start_server(_handle_client, host, port)
    print("  HTTP Dashboard  : http://localhost:{}".format(port))
    await asyncio.Future()
