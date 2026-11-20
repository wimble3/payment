from uuid import uuid4

import pytest_asyncio

from app.domain.entities.payments import Payment, PaymentDetails
from app.domain.use_cases.payments import CreatePaymentUseCase
from app.infra.base.database import SessionFactory
from tests.support.types import JsonObject


@pytest_asyncio.fixture
async def queued_payment(database: SessionFactory, payment_body: JsonObject) -> Payment:
    return await CreatePaymentUseCase(database).execute(
        PaymentDetails.model_validate(payment_body), str(uuid4())
    )
