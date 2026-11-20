from abc import ABC, abstractmethod
from uuid import UUID

from app.domain.entities.payments import Payment, PaymentStatusEnum


class PaymentGateway(ABC):
    @abstractmethod
    async def charge(self, payment_id: UUID) -> PaymentStatusEnum:
        pass


class WebhookGateway(ABC):
    @abstractmethod
    async def send(self, payment: Payment, event_id: UUID) -> None:
        pass
