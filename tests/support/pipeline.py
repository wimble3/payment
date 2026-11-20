import asyncio
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Self
from uuid import UUID, uuid4

import httpx
from sqlalchemy import select

from app.config import Settings
from app.domain.ports import PaymentGateway
from app.infra.base.database import SessionFactory
from app.infra.models import OutboxModel, PaymentModel
from app.infra.outbox import OutboxRelay
from app.transport.faststream.app import create_consumer_app
from tests.support.types import JsonObject, RabbitSession
from tests.support.webhooks import webhook_server


async def drive(
    relay: OutboxRelay, predicate: Callable[[], Awaitable[bool]], timeout: float = 15
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        await relay.publish_batch()
        if await predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("Pipeline did not reach the expected state")


class Pipeline:
    def __init__(
        self,
        settings: Settings,
        database: SessionFactory,
        client: httpx.AsyncClient,
        payment_body: JsonObject,
        rabbit: RabbitSession,
    ) -> None:
        self.settings, self.database, self.client = settings, database, client
        self.payment_body, self.rabbit = payment_body, rabbit
        self.broker, self.channel = rabbit
        self.relay = OutboxRelay(self.broker, settings, database)
        self.dead_messages: list[JsonObject] = []

    @asynccontextmanager
    async def run(
        self,
        responses: Sequence[int] = (200,),
        success_rate: float = 1,
        gateway: PaymentGateway | None = None,
    ) -> AsyncIterator[Self]:
        self.settings.gateway_success_rate = success_rate
        async with AsyncExitStack() as stack:
            self.url, self.received = await stack.enter_async_context(webhook_server(responses))
            consumer = create_consumer_app(self.settings, self.database, payment_gateway=gateway)
            await consumer.start()
            stack.push_async_callback(consumer.stop)
            yield self

    async def create(self, url: str | None = None) -> None:
        body = dict(self.payment_body, webhook_url=url or self.url)
        response = await self.client.post(
            "/api/v1/payments", json=body, headers={"Idempotency-Key": str(uuid4())}
        )
        assert response.status_code == 202, response.text
        self.payment_id = UUID(response.json()["payment_id"])

    async def payment(self) -> PaymentModel:
        async with self.database() as session:
            payment = await session.get(PaymentModel, self.payment_id)
            assert payment is not None
            return payment

    async def delivered(self) -> bool:
        return (await self.payment()).webhook_delivered_at is not None

    async def wait_for_delivery(self) -> None:
        await drive(self.relay, self.delivered)

    async def poll_dead(self, topic: str) -> bool:
        queue = await self.channel.get_queue(f"{topic}.dead.dlq")
        message = await queue.get(fail=False)
        if message:
            self.dead_messages.append(json.loads(message.body))
            await message.ack()
            return True
        return False

    async def wait_for_dead(self, topic: str) -> None:
        async def finished() -> bool:
            return await self.poll_dead(topic)

        await drive(self.relay, finished)

    async def events(self, topic: str | None = None) -> Sequence[OutboxModel]:
        stmt = select(OutboxModel)
        if topic:
            stmt = stmt.where(OutboxModel.topic == topic)
        async with self.database() as session:
            return (await session.scalars(stmt)).all()

    async def redeliver(self, topic: str | None = None) -> None:
        for event in await self.events(topic):
            await self.broker.publish(event.payload, queue=event.topic)
        await asyncio.sleep(0.2)

    async def assert_delivered(self, expected: str) -> None:
        response = await self.client.get(f"/api/v1/payments/{self.payment_id}")
        result = response.json()
        assert result["status"] == expected
        assert result["processed_at"] is not None
        assert len(self.received) == 1
        body, headers, _ = self.received[0]
        assert body["status"] == expected
        assert headers["idempotency-key"] == body["event_id"]

    def assert_retry_timing(self) -> None:
        assert len(self.received) == 3
        assert len({item[1]["idempotency-key"] for item in self.received}) == 1
        base = self.settings.retry_base_seconds
        assert self.received[1][2] - self.received[0][2] >= base * 0.8
        assert self.received[2][2] - self.received[1][2] >= base * 2 * 0.8

    async def assert_retry_result(self, dead: bool) -> None:
        payment = await self.payment()
        assert payment.status == "succeeded"
        assert (payment.webhook_delivered_at is None) == dead
        events = await self.events()
        assert (
            len([event for event in events if event.topic.startswith("payments.status.retry.")])
            == 2
        )
        if dead:
            assert self.dead_messages[0]["attempt"] == 3
