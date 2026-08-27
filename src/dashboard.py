from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as aioredis
import json
import os

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
CACHE_KEY = "market:latest"
CHANNEL_NAME = "market:signals"

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:5173"], allow_methods=["*"], allow_headers=["*"])
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
