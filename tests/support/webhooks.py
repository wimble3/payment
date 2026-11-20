import asyncio
import json
import time
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager

from tests.support.types import ReceivedWebhook


async def read_request(reader: asyncio.StreamReader) -> ReceivedWebhook:
    head = await reader.readuntil(b"\r\n\r\n")
    headers = {}
    for line in head.decode().split("\r\n")[1:]:
        if ":" in line:
            key, value = line.split(":", 1)
            headers[key.lower()] = value.strip()
    body = await reader.readexactly(int(headers.get("content-length", 0)))
    return json.loads(body), headers, time.monotonic()


class WebhookReceiver:
    def __init__(self, responses: Sequence[int]) -> None:
        self.responses: Sequence[int] = responses
        self.received: list[ReceivedWebhook] = []

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            self.received.append(await read_request(reader))
            code = self.responses[min(len(self.received) - 1, len(self.responses) - 1)]
            status = f"HTTP/1.1 {code} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            writer.write(status.encode())
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


@asynccontextmanager
async def webhook_server(
    responses: Sequence[int],
) -> AsyncIterator[tuple[str, list[ReceivedWebhook]]]:
    receiver = WebhookReceiver(responses)
    server = await asyncio.start_server(receiver.handle, "0.0.0.0", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}/webhook", receiver.received
    finally:
        server.close()
        await server.wait_closed()


async def unused_webhook_url() -> str:
    server = await asyncio.start_server(lambda r, w: w.close(), "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    server.close()
    await server.wait_closed()
    return f"http://127.0.0.1:{port}/webhook"
