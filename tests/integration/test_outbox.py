from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from app.config import Settings
from app.domain.entities.payments import Payment
from app.infra.base.database import SessionFactory
from app.infra.models import OutboxModel
from app.infra.outbox import OutboxRelay


async def assert_failed_and_reschedule(database: SessionFactory) -> None:
    async with database.begin() as session:
        event = (await session.scalars(select(OutboxModel))).one()
        assert event.published_at is None
        assert event.publish_attempts == 1
        assert event.last_error == "ConnectionError"
        event.available_at = datetime.now(UTC) - timedelta(seconds=1)


async def assert_published(database: SessionFactory) -> None:
    async with database() as session:
        event = (await session.scalars(select(OutboxModel))).one()
        assert event.published_at is not None
        assert event.publish_attempts == 2
        assert event.last_error is None


async def test_publish_failure_is_retried(
    settings: Settings, database: SessionFactory, queued_payment: Payment
) -> None:
    broker = AsyncMock()
    broker.publish.side_effect = ConnectionError("RabbitMQ unavailable")
    relay = OutboxRelay(broker, settings, database)
    assert await relay.publish_batch() == 1
    await assert_failed_and_reschedule(database)
    broker.publish.side_effect = None
    await relay.publish_batch()
    await assert_published(database)


async def test_skip_locked(
    settings: Settings, database: SessionFactory, queued_payment: Payment
) -> None:
    async with database.begin() as session:
        await session.scalar(select(OutboxModel).with_for_update())
        relay = OutboxRelay(AsyncMock(), settings, database)
        assert await relay.publish_batch() == 0


async def test_confirmation_followed_by_rollback_can_be_republished(
    settings: Settings, database: SessionFactory, queued_payment: Payment
) -> None:
    broker = AsyncMock()
    relay = OutboxRelay(broker, settings, database)
    await relay.publish_batch()
    async with database.begin() as session:
        event = (await session.scalars(select(OutboxModel))).one()
        event.published_at = None
    await relay.publish_batch()
    assert broker.publish.call_count == 2
    first, second = broker.publish.call_args_list
    assert first.kwargs["message_id"] == second.kwargs["message_id"]


async def test_unexpected_publish_error_propagates(
    settings: Settings, database: SessionFactory, queued_payment: Payment
) -> None:
    broker = AsyncMock()
    broker.publish.side_effect = ValueError("Unexpected publisher bug")
    relay = OutboxRelay(broker, settings, database)
    with pytest.raises(ValueError, match="Unexpected publisher bug"):
        await relay.publish_batch()
    async with database() as session:
        event = (await session.scalars(select(OutboxModel))).one()
        assert event.published_at is None
        assert event.publish_attempts == 0
