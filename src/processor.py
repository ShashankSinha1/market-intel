from redis import ResponseError
import redis.asyncio as redis
import asyncio

STREAM_NAME = "market:ticks"
GROUP_NAME= "processors"
CONSUMER_NAME= "processor-1"

async def process_group() -> None:
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)

    try:
        await r.xgroup_create(STREAM_NAME, GROUP_NAME, id="$", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
        print("Group already exists")

    while True:
        response = await r.xreadgroup(
            GROUP_NAME, CONSUMER_NAME, {STREAM_NAME: ">"}, count=10, block=5000
        )

        for stream_name, messages in response:
            for message_id, fields in messages:
                print(fields)
                await r.xack(STREAM_NAME, GROUP_NAME, message_id)

if __name__ == "__main__":
    asyncio.run(process_group())