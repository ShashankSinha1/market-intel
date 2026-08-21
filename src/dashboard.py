from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import redis.asyncio as aioredis
import json

REDIS_URL = "redis://localhost:6379"
CACHE_KEY = "market:latest"
CHANNEL_NAME = "market:signals"

app = FastAPI()
r = aioredis.from_url(REDIS_URL, decode_responses=True)

@app.get("/api/signals")
async def get_signals():
    dumps = await r.hgetall(CACHE_KEY)
    return {product_id : json.loads(value) for product_id, value in dumps.items()}

@app.websocket("/ws/signals")
async def live_signals(websocket: WebSocket):
    await websocket.accept()
    pubsub = r.pubsub()
    await pubsub.subscribe(CHANNEL_NAME)

    print(f"[*] Subscribed to Redis channel: '{CHANNEL_NAME}'")

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                print(f"[Subscriber Received]: Data = {message['data']}")
                await websocket.send_text(message["data"])

    except WebSocketDisconnect:
        print("Client disconnected")

    finally:
        await pubsub.unsubscribe(CHANNEL_NAME)
        await pubsub.close()
