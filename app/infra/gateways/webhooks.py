from uuid import UUID

import httpx
from pydantic import JsonValue

from app.domain.entities.payments import Payment
from app.domain.ports import WebhookGateway


def webhook_payload(payment: Payment, event_id: UUID) -> dict[str, JsonValue]:
    return {
        "event_id": str(event_id),
        "event_type": "payment.processed",
        "payment_id": str(payment.payment_id),
        "status": payment.status,
        "amount": format(payment.amount, ".2f"),
        "currency": payment.currency,
        "metadata": payment.metadata,
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }


class HTTPWebhookGateway(WebhookGateway):
    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def send(self, payment: Payment, event_id: UUID) -> None:
        response = await self.client.post(
            str(payment.webhook_url),
            json=webhook_payload(payment, event_id),
            headers={"Idempotency-Key": str(event_id)},
        )
        if not 200 <= response.status_code < 300:
            raise httpx.HTTPStatusError(
                "Webhook must return 2xx", request=response.request, response=response
            )
