from collections.abc import Sequence
from uuid import UUID, uuid4

import pytest
from faststream.rabbit import RabbitQueue
from pydantic import JsonValue

from app.domain.entities.payments import PaymentStatusEnum
from app.domain.ports import PaymentGateway
from app.transport.faststream.topology import Route
from tests.support.pipeline import Pipeline
from tests.support.webhooks import unused_webhook_url

pytestmark = pytest.mark.e2e


@pytest.mark.parametrize("success_rate,expected", [(1, "succeeded"), (0, "failed")])
async def test_api_to_payment_to_webhook(
    pipeline: Pipeline, success_rate: float, expected: str
) -> None:
    async with pipeline.run(success_rate=success_rate):
        await pipeline.create()
        await pipeline.wait_for_delivery()
        await pipeline.assert_delivered(expected)
        await pipeline.redeliver()
        assert len(pipeline.received) == 1


@pytest.mark.parametrize("responses,dead", [([503, 503, 200], False), ([503], True), ([302], True)])
async def test_webhook_retries_and_dlq(
    pipeline: Pipeline, responses: Sequence[int], dead: bool
) -> None:
    async with pipeline.run(responses=responses):
        await pipeline.create()
        if dead:
            await pipeline.wait_for_dead("payments.status")
        else:
            await pipeline.wait_for_delivery()
        pipeline.assert_retry_timing()
        await pipeline.assert_retry_result(dead)
        await pipeline.redeliver("payments.status")
        assert len(pipeline.received) == 3


async def test_processing_failure_dlq(pipeline: Pipeline) -> None:
    async with pipeline.run():
        event = {"event_id": str(uuid4()), "payment_id": str(uuid4()), "attempt": 1}
        await pipeline.broker.publish(event, queue="payments.new")
        await pipeline.wait_for_dead("payments.new")
        assert pipeline.dead_messages[0]["attempt"] == 3
        assert pipeline.dead_messages[0]["error"] == "DBNotFound"


@pytest.mark.parametrize(
    "body,content_type",
    [
        ({"invalid": True}, None),
        (b"{", "application/json"),
        (b"\xff", "application/octet-stream"),
    ],
)
async def test_poison_message_to_dlq(
    pipeline: Pipeline, body: JsonValue, content_type: str | None
) -> None:
    async with pipeline.run():
        await pipeline.broker.publish(body, queue="payments.new", content_type=content_type)
        await pipeline.wait_for_dead("payments.new")
        assert pipeline.dead_messages[0]["error"] == "Invalid payment event"


async def test_webhook_connection_refused(pipeline: Pipeline) -> None:
    async with pipeline.run():
        await pipeline.create(await unused_webhook_url())
        await pipeline.wait_for_dead("payments.status")
        assert pipeline.dead_messages[0]["attempt"] == 3
        assert pipeline.dead_messages[0]["error"] == "ConnectError"


async def test_broker_return_does_not_mark_outbox_published(pipeline: Pipeline) -> None:
    await pipeline.create(str(pipeline.payment_body["webhook_url"]))
    pipeline.relay.routes["payments.new"] = Route(RabbitQueue(f"missing-{uuid4()}"))
    await pipeline.relay.publish_batch()
    event = (await pipeline.events())[0]
    assert event.published_at is None
    assert event.last_error is not None
    assert event.publish_attempts == 1


class RecoveringGateway(PaymentGateway):
    calls = 0

    async def charge(self, payment_id: UUID) -> PaymentStatusEnum:
        self.calls += 1
        if self.calls < 3:
            raise ConnectionError("Gateway unavailable")
        return PaymentStatusEnum.SUCCEEDED


async def test_payment_gateway_recovers_on_third_attempt(pipeline: Pipeline) -> None:
    gateway = RecoveringGateway()
    async with pipeline.run(gateway=gateway):
        await pipeline.create()
        await pipeline.wait_for_delivery()
        assert gateway.calls == 3
        assert len(pipeline.received) == 1
        await pipeline.redeliver("payments.new")
        assert gateway.calls == 3


class FailingGateway(PaymentGateway):
    def __init__(self, error_type: type[Exception]) -> None:
        self.error_type, self.calls = error_type, 0

    async def charge(self, payment_id: UUID) -> PaymentStatusEnum:
        self.calls += 1
        raise self.error_type("Unexpected gateway failure")


@pytest.mark.parametrize("error_type", [TypeError, RuntimeError])
async def test_unexpected_consumer_error_reaches_dlq(
    pipeline: Pipeline, error_type: type[Exception]
) -> None:
    gateway = FailingGateway(error_type)
    async with pipeline.run(gateway=gateway):
        await pipeline.create()
        await pipeline.wait_for_dead("payments.new")
        assert gateway.calls == 3
        assert pipeline.dead_messages[0]["error"] == error_type.__name__
        assert pipeline.received == []
