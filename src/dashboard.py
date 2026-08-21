import json

from fastapi import FastAPI
import redis.asyncio as redis

CACHE_KEY = "market:latest"

app = FastAPI()
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

@app.get("/api/signals")
async def get_signals():
    dumps = await r.hgetall(CACHE_KEY)
    return {product_id : json.loads(value) for product_id, value in dumps.items()}