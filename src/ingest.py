"""
Phase 0: connect to Coinbase's public WebSocket feed, subscribe to the
ticker channel for multiple products, and print each tick as it arrives.

No Redis, no persistence yet — just proving out the async connection
and message loop.
"""

import asyncio
import json
import redis.asyncio as redis

import websockets

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"

# One connection handles all of these — Coinbase multiplexes ticks for
# every subscribed product over the same socket, distinguished by the
# "product_id" field in each message.
PRODUCT_IDS = ["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD", "AVAX-USD"]
STREAM_NAME = "market:ticks"

async def stream_ticks() -> None:
    r = redis.Redis(host="localhost", port=6379)

    while True:
        try:
            async with websockets.connect(COINBASE_WS_URL) as ws:
                subscribe_message = {
                    "type": "subscribe",
                    "product_ids": PRODUCT_IDS,
                    "channels": ["ticker"],
                }
                await ws.send(json.dumps(subscribe_message))

                async for raw_message in ws:
                    data = json.loads(raw_message)

                    # Coinbase sends one "subscriptions" ack first, then a stream
                    # of "ticker" messages after that — skip anything that isn't
                    # a tick.
                    if data.get("type") != "ticker":
                        continue

                    print(f"{data['product_id']:<10} {data['price']:>12} @ {data['time']}")
                    await r.xadd(STREAM_NAME, {
                        "product_id": data["product_id"],
                        "price": data["price"],
                        "time": data["time"],
                    })
        except websockets.exceptions.ConnectionClosed:
            print("Connection dropped :(")
            await asyncio.sleep(3)

        except OSError:
            print("Couldn't connect")
            await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(stream_ticks())