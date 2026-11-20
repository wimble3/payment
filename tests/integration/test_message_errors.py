from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.domain.entities.events import PaymentEvent
from app.domain.use_cases.base import IUseCase
from app.domain.use_cases.messages import HandlePaymentMessageUseCase, SchedulePaymentFailureUseCase
from app.infra.base.database import SessionFactory
from app.infra.models import OutboxModel
from app.transport.faststream.app import consume_message


class BrokenHandler(IUseCase[[PaymentEvent], None]):
    async def execute(self, event: PaymentEvent) -> None:
        raise TypeError("Unexpected programming error")


async def test_programming_error_does_not_schedule_retry(database: SessionFactory) -> None:
    event = PaymentEvent(event_id=uuid4(), payment_id=uuid4())
    case = HandlePaymentMessageUseCase(database, BrokenHandler(), "payments.new")
    with pytest.raises(TypeError, match="Unexpected programming error"):
        await case.execute(event.model_dump(mode="json"))
    async with database() as session:
        assert await session.scalar(select(func.count()).select_from(OutboxModel)) == 0


async def test_consumer_requeues_when_retry_cannot_be_saved(database: SessionFactory) -> None:
    event = PaymentEvent(event_id=uuid4(), payment_id=uuid4())
    case = HandlePaymentMessageUseCase(database, BrokenHandler(), "payments.new")
    failures = AsyncMock(spec=SchedulePaymentFailureUseCase)
    failures.execute.side_effect = RuntimeError("Cannot persist retry")
    message = AsyncMock()
    message.message_id = str(event.event_id)
    await consume_message(case, failures, event.model_dump(mode="json"), message)
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()
