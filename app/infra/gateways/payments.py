import asyncio
import hashlib
import random
from uuid import UUID

from app.config import Settings
from app.domain.entities.payments import PaymentStatusEnum
from app.domain.ports import PaymentGateway


class MockPaymentGateway(PaymentGateway):
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def charge(self, payment_id: UUID) -> PaymentStatusEnum:
        await asyncio.sleep(
            random.uniform(self.settings.gateway_min_seconds, self.settings.gateway_max_seconds)
        )
        value = int.from_bytes(hashlib.sha256(payment_id.bytes).digest(), "big") / 2**256
        return (
            PaymentStatusEnum.SUCCEEDED
            if value < self.settings.gateway_success_rate
            else PaymentStatusEnum.FAILED
        )
