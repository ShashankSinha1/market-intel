"""
Throwaway client for manually testing the /ws/signals WebSocket
endpoint in dashboard.py. Connects and prints whatever it receives.
"""

import asyncio

import websockets


async def main() -> None:
    async with websockets.connect("ws://localhost:8000/ws/signals") as ws:
        async for message in ws:
            print(message)


if __name__ == "__main__":
    asyncio.run(main())
