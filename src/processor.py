import asyncio
import json
from collections import deque
from datetime import datetime, timedelta

import pymongo.errors
import redis.asyncio as aioredis
from motor.motor_asyncio import AsyncIOMotorClient
from redis import ResponseError
from redis.exceptions import RedisError

REDIS_URL = "redis://localhost:6379"
STREAM_NAME = "market:ticks"
GROUP_NAME = "processors"
CONSUMER_NAME = "processor-1"
CHANNEL_NAME = "market:signals"
CACHE_KEY = "market:latest"
WINDOW = timedelta(seconds=30)


async def process_group() -> None:
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client["market_intel"]
    collection = db["ticks"]
    last_prices = {}
    price_history = {}

    try:
        await r.xgroup_create(STREAM_NAME, GROUP_NAME, id="$", mkstream=True)

    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
        print("Group already exists")

    try:
        await db.create_collection(
            "ticks",
            timeseries={
                "timeField": "time",
                "metaField": "product_id",
                "granularity": "seconds",
            },
        )
    except pymongo.errors.CollectionInvalid as e:
        print(f"Collection already exists: {e}")

    async def process_message(message_id, fields):
        fields["time"] = datetime.fromisoformat(fields["time"])
        current_price = float(fields["price"])
        fields["price"] = current_price
        previous_price = last_prices.get(fields["product_id"])
        product_deque = price_history.setdefault(fields["product_id"], deque())

        product_deque.append((fields["time"], current_price))

        while product_deque and fields["time"] - WINDOW > product_deque[0][0]:
            product_deque.popleft()

        prices_sum = sum(price for _, price in product_deque)
        avg = prices_sum / len(product_deque)
        avg = round(avg, 6)

        if previous_price is not None:
            delta = current_price - previous_price
            delta = round(delta, 6)
        else:
            delta = 0

        fields["delta"] = delta
        last_prices[fields["product_id"]] = current_price
        fields["average"] = avg
        json_payload = {
            "product_id": fields["product_id"],
            "price": current_price,
            "time": fields["time"].isoformat(),
            "delta": delta,
            "average": avg,
        }
        print(fields)
    
        try:
            payload = json.dumps(json_payload)
            await collection.insert_one(fields)
            await r.publish(CHANNEL_NAME, payload)
            await r.hset(CACHE_KEY, fields["product_id"], payload)
            await r.xack(STREAM_NAME, GROUP_NAME, message_id)

        except (pymongo.errors.PyMongoError, RedisError) as e:
            print(f"Error: {e}")

    while True:
        try:
            _, claimed_messages, _ = await r.xautoclaim(
                STREAM_NAME, GROUP_NAME, CONSUMER_NAME, min_idle_time=30000
            )

            for message_id, fields in claimed_messages:
                await process_message(message_id, fields)

        except RedisError as e:
            print(f"{e}")
            await asyncio.sleep(3)
            continue

        try:
            response = await r.xreadgroup(
                GROUP_NAME, CONSUMER_NAME, {STREAM_NAME: ">"}, count=10, block=5000
            )

        except RedisError as e:
            print(f"{e}")
            await asyncio.sleep(3)
            continue

        for _, messages in response:
            for message_id, fields in messages:  # type: ignore
                await process_message(message_id, fields)


if __name__ == "__main__":
    asyncio.run(process_group())
