import hashlib
import json
from uuid import UUID, uuid5

from app.domain.entities.events import PaymentEvent
from app.domain.entities.payments import Payment, PaymentDetails, PaymentStatusEnum
from app.domain.exceptions import PaymentNotProcessed
from app.domain.ports import PaymentGateway, WebhookGateway
from app.domain.use_cases.base import IUseCase
from app.infra.base.database import SessionFactory
from app.infra.repositories.outbox import SQLEventRepository
from app.infra.repositories.payments import SQLPaymentRepository


def request_fingerprint(details: PaymentDetails) -> str:
    data = details.model_dump(mode="json")

    data["amount"] = format(details.amount, ".2f")
    return hashlib.sha256(
        json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class CreatePaymentUseCase(IUseCase[[PaymentDetails, str], Payment]):
    def __init__(self, session_factory: SessionFactory) -> None:
        self.session_factory = session_factory

    async def execute(self, details: PaymentDetails, idempotency_key: str) -> Payment:
        async with self.session_factory.begin() as session:
            payment = await SQLPaymentRepository(session).create(
                details, idempotency_key, request_fingerprint(details)
            )
            event_id = uuid5(payment.payment_id, "payments.new")
            event = PaymentEvent(event_id=event_id, payment_id=payment.payment_id)
            await SQLEventRepository(session).add(
                event_id, "payments.new", event.model_dump(mode="json")
            )
            return payment


class GetPaymentUseCase(IUseCase[[UUID], Payment]):
    def __init__(self, session_factory: SessionFactory) -> None:
        self.session_factory = session_factory

    async def execute(self, payment_id: UUID) -> Payment:
        async with self.session_factory.begin() as session:
            return await SQLPaymentRepository(session).get(payment_id)


class ProcessPaymentUseCase(IUseCase[[PaymentEvent], None]):
    def __init__(self, session_factory: SessionFactory, gateway: PaymentGateway) -> None:
        self.session_factory, self.gateway = session_factory, gateway

    async def execute(self, event: PaymentEvent) -> None:
        async with self.session_factory.begin() as session:
            payment = await SQLPaymentRepository(session).get(event.payment_id, lock=True)
            if payment.status != PaymentStatusEnum.PENDING:
                return
            status = await self.gateway.charge(payment.payment_id)
            await SQLPaymentRepository(session).set_status(payment.payment_id, status)
            event_id = uuid5(payment.payment_id, "payments.status")
            status_event = PaymentEvent(event_id=event_id, payment_id=payment.payment_id)
            await SQLEventRepository(session).add(
                event_id, "payments.status", status_event.model_dump(mode="json")
            )


class SendPaymentWebhookUseCase(IUseCase[[PaymentEvent], None]):
    def __init__(self, session_factory: SessionFactory, gateway: WebhookGateway) -> None:
        self.session_factory, self.gateway = session_factory, gateway

    async def execute(self, event: PaymentEvent) -> None:
        async with self.session_factory.begin() as session:
            payment = await SQLPaymentRepository(session).get(event.payment_id, lock=True)
            if payment.webhook_delivered_at is not None:
                return
            if payment.status == PaymentStatusEnum.PENDING:
                raise PaymentNotProcessed("Payment has not been processed yet")
            await self.gateway.send(payment, event.event_id)
            await SQLPaymentRepository(session).mark_delivered(payment.payment_id)
