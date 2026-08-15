import asyncio

async def count_up(letter):
    for i in range(5):
        print(f"{letter}{i}")
        await asyncio.sleep(0.5)
        print(f"Yo {i}")
    

async def main():
    await asyncio.gather(
        count_up("A"),
        count_up("B")
    )

asyncio.run(main())