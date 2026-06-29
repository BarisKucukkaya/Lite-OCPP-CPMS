"""
Async HTTP server for the dashboard (asyncio.start_server — zero extra deps).

Endpoints:
  GET  /               -> static/index.html
  GET  /api/chargers   -> current charger state as JSON
  GET  /events         -> SSE stream (text/event-stream)
  POST /api/action     -> send RemoteStart/Stop to a charge point
"""
import asyncio
import json
import os
import state
import ocpp_server

_STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

_CORS_HEADERS = (
    b"Access-Control-Allow-Origin: *\r\n"
    b"Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
    b"Access-Control-Allow-Headers: Content-Type\r\n"
)


# ---------------------------------------------------------------------------
# Low-level write helpers
# ---------------------------------------------------------------------------

async def _write_response(writer, status: int, extra_headers: bytes, body: bytes,
                          content_type: bytes = b"text/plain"):
    status_line = f"HTTP/1.1 {status} {_reason(status)}\r\n".encode()
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


async def _write_json(writer, obj: dict, status: int = 200):
    body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    await _write_response(writer, status, b"", body,
                          content_type=b"application/json; charset=utf-8")


async def _write_sse_event(writer, event: dict):
    data = json.dumps(event, ensure_ascii=False)
    writer.write(("data: " + data + "\n\n").encode("utf-8"))
    await writer.drain()


def _reason(code: int) -> str:
    return {200: "OK", 204: "No Content", 400: "Bad Request",
            404: "Not Found", 502: "Bad Gateway", 503: "Service Unavailable"}.get(code, "")


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

async def _serve_file(writer):
    filepath = os.path.join(_STATIC_DIR, "index.html")
    try:
        with open(filepath, "rb") as f:
            body = f.read()
    except FileNotFoundError:
        await _write_response(writer, 404, b"", b"index.html not found")
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


async def _handle_action(writer, body_bytes: bytes):
    try:
        data = json.loads(body_bytes)
    except (ValueError, TypeError):
        await _write_json(writer, {"error": "Invalid JSON body"}, 400)
        return

    cp_id = data.get("cp_id", "")
    action = data.get("action", "")
    connector_id = int(data.get("connector_id", 1))

    if action not in ("RemoteStartTransaction", "RemoteStopTransaction"):
        await _write_json(writer, {"error": "action must be RemoteStartTransaction or RemoteStopTransaction"}, 400)
        return

    try:
        result = await ocpp_server.remote_action(cp_id, action, connector_id)
        await _write_json(writer, {"status": "ok", "result": result})
    except Exception as e:
        await _write_json(writer, {"error": str(e)}, 502)


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

        elif method == "GET" and path in ("/", "/index.html"):
            await _serve_file(writer)

        elif method == "GET" and path == "/api/chargers":
            await _serve_chargers(writer)

        elif method == "GET" and path == "/events":
            await _serve_sse(writer)
            return  # SSE handler closes connection itself

        elif method == "POST" and path == "/api/action":
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
    print(f"  HTTP Dashboard  : http://localhost:{port}")
    await asyncio.Future()
