"""
Lite OCPP 1.6J CPMS - entry point.

Starts two servers in a single asyncio event loop:
  - OCPP WebSocket server  on port 9000
  - HTTP dashboard server  on port 8000

Run with:
    python3 main.py
"""
import asyncio
import db
import state
import ocpp_server
import http_server


async def main():
    print()
    print("=" * 48)
    print("   Lite OCPP 1.6J CPMS")
    print("=" * 48)

    db.init()
    loaded = db.load_transactions()
    state.transactions.update(loaded)
    if loaded:
        state._next_transaction_id = max(loaded.keys()) + 1

    await asyncio.gather(
        ocpp_server.run_server(host="0.0.0.0", port=9000),
        http_server.run_server(host="0.0.0.0", port=8000),
    )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\nStopped.")
