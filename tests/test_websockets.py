import asyncio
import websockets as websockets


def test_url(url, data="test/test websockets"):
    async def inner():
        async with websockets.connect(url) as websocket:
            await websocket.send(data)

    return asyncio.get_event_loop().run_until_complete(inner())


# test_url("ws://127.0.0.1:8000/ws/registration/123")
test_url("ws://127.0.0.1:9000")
