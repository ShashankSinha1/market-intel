import asyncio
from collections import deque
from datetime import datetime, timedelta

import pymongo.errors
import redis.asyncio as redis
from motor.motor_asyncio import AsyncIOMotorClient
from redis import ResponseError

STREAM_NAME = "market:ticks"
GROUP_NAME = "processors"
CONSUMER_NAME = "processor-1"
WINDOW = timedelta(seconds=30)


async def process_group() -> None:
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
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

    while True:
        response = await r.xreadgroup(
            GROUP_NAME, CONSUMER_NAME, {STREAM_NAME: ">"}, count=10, block=5000
        )

        for _, messages in response:
            for message_id, fields in messages:  # type: ignore
                print(fields)
                fields["time"] = datetime.fromisoformat(fields["time"])
                current_price = float(fields["price"])
                previous_price = last_prices.get(fields["product_id"])
                product_deque = price_history.setdefault(fields["product_id"], deque())

                product_deque.append((fields["time"], current_price))

                while product_deque and fields["time"] - WINDOW > product_deque[0][0]:
                    product_deque.popleft()

                prices_sum = sum(price for _, price in product_deque)
                avg = prices_sum / len(product_deque)

                if previous_price is not None:
                    delta = current_price - previous_price
                else:
                    delta = 0

                fields["delta"] = delta
                last_prices[fields["product_id"]] = current_price
                fields["average"] = avg

                try:
                    await collection.insert_one(fields)
                    await r.xack(STREAM_NAME, GROUP_NAME, message_id)

                except pymongo.errors.PyMongoError as e:
                    print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(process_group())
