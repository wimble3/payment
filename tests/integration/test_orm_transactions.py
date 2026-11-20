import asyncio
from decimal import Decimal
from uuid import UUID, uuid4, uuid5

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.domain.entities.events import PaymentEvent
from app.domain.entities.payments import Payment, PaymentDetails, PaymentStatusEnum
from app.domain.ports import PaymentGateway
from app.domain.use_cases.payments import CreatePaymentUseCase, ProcessPaymentUseCase
from app.infra.base.database import SessionFactory
from app.infra.models import OutboxModel, PaymentModel
from app.infra.repositories.outbox import SQLEventRepository
from tests.support.types import JsonObject


async def add_duplicate_with_marker(factory: SessionFactory, event_id: UUID, index: int) -> None:
    async with factory.begin() as session:
        repository = SQLEventRepository(session)
        await repository.add(event_id, "payments.new", {"original": True})
        await repository.add(uuid5(event_id, str(index)), "payments.new", {"marker": index})


async def test_concurrent_outbox_duplicates_preserve_transactions(database: SessionFactory) -> None:
    event_id = uuid4()
    await asyncio.gather(
        *(add_duplicate_with_marker(database, event_id, index) for index in range(10))
    )
    async with database() as session:
        assert await session.scalar(select(func.count()).select_from(OutboxModel)) == 11
        event = await session.get(OutboxModel, event_id)
        assert event is not None
        assert event.payload == {"original": True}


class CountingGateway(PaymentGateway):
    calls: int = 0

    async def charge(self, payment_id: UUID) -> PaymentStatusEnum:
        self.calls += 1
        await asyncio.sleep(0.05)
        return PaymentStatusEnum.SUCCEEDED


async def test_concurrent_processing_locks_payment(
    database: SessionFactory, queued_payment: Payment
) -> None:
    gateway = CountingGateway()
    case = ProcessPaymentUseCase(database, gateway)
    event = PaymentEvent(event_id=uuid4(), payment_id=queued_payment.payment_id)
    await asyncio.gather(case.execute(event), case.execute(event))
    assert gateway.calls == 1
    async with database() as session:
        payment = await session.get(PaymentModel, queued_payment.payment_id)
        assert payment is not None and payment.status == PaymentStatusEnum.SUCCEEDED
        assert await session.scalar(select(func.count()).select_from(OutboxModel)) == 2


async def test_unrelated_integrity_error_is_not_treated_as_duplicate(
    database: SessionFactory, payment_body: JsonObject, queued_payment: Payment
) -> None:
    details = PaymentDetails.model_validate(payment_body).model_copy(update={"amount": Decimal(-1)})
    with pytest.raises(IntegrityError):
        await CreatePaymentUseCase(database).execute(details, queued_payment.idempotency_key)
    async with database() as session:
        payment = await session.get(PaymentModel, queued_payment.payment_id)
        assert payment is not None and payment.amount == Decimal("125.50")
        assert await session.scalar(select(func.count()).select_from(PaymentModel)) == 1
