import pymongo.errors
from redis import ResponseError
import redis.asyncio as redis
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime

STREAM_NAME = "market:ticks"
GROUP_NAME= "processors"
CONSUMER_NAME= "processor-1"

async def process_group() -> None:
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client["market_intel"]
    collection = db["ticks"]
    
    try:
        await r.xgroup_create(STREAM_NAME, GROUP_NAME, id="$", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
        print("Group already exists")

    try:
        await db.create_collection("ticks", timeseries={"timeField": "time", "metaField": "product_id", "granularity": "seconds"})
    except pymongo.errors.CollectionInvalid as e:
        print(f"Collection already exists: {e}")

    while True:
        response = await r.xreadgroup(
            GROUP_NAME, CONSUMER_NAME, {STREAM_NAME: ">"}, count=10, block=5000
        )

        for stream_name, messages in response:
            for message_id, fields in messages: # type: ignore
                print(fields)
                fields["time"] = datetime.fromisoformat(fields["time"])

                try:
                    await collection.insert_one(fields)
                    await r.xack(STREAM_NAME, GROUP_NAME, message_id)

                except pymongo.errors.PyMongoError as e:
                    print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(process_group())