from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.entities.payments import Payment, PaymentDetails, PaymentStatusEnum
from app.domain.exceptions import IdempotencyConflict
from app.infra.base.exceptions import DBNotFound, is_unique_violation
from app.infra.models import PaymentModel


def to_payment(row: PaymentModel) -> Payment:
    values = {field: getattr(row, field) for field in Payment.model_fields if field != "metadata"}
    values["metadata"] = row.metadata_json
    return Payment.model_validate(values)


def new_payment(details: PaymentDetails, key: str, request_hash: str) -> PaymentModel:
    return PaymentModel(
        idempotency_key=key,
        request_hash=request_hash,
        amount=details.amount,
        currency=details.currency,
        description=details.description,
        metadata_json=details.metadata,
        webhook_url=str(details.webhook_url),
    )


class SQLPaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(self, details: PaymentDetails, key: str, request_hash: str) -> Payment:
        row = new_payment(details, key, request_hash)
        try:
            async with self.session.begin_nested():
                self.session.add(row)
                await self.session.flush()
        except IntegrityError as error:
            if not is_unique_violation(error, "payments_idempotency_key_key"):
                raise
            row = await self._get_by_key(key, request_hash)
        return to_payment(row)

    async def _get_by_key(self, key: str, request_hash: str) -> PaymentModel:
        stmt = select(PaymentModel).where(PaymentModel.idempotency_key == key)
        row = (await self.session.scalars(stmt)).one()
        if row.request_hash != request_hash:
            raise IdempotencyConflict(
                "Idempotency-Key has already been used with a different request"
            )
        return row

    async def _get_model(self, payment_id: UUID, *, lock: bool = False) -> PaymentModel:
        row = await self.session.get(
            PaymentModel,
            payment_id,
            with_for_update=True if lock else None,
            populate_existing=lock,
        )
        if row is None:
            raise DBNotFound(f"Payment {payment_id} not found")
        return row

    async def get(self, payment_id: UUID, *, lock: bool = False) -> Payment:
        return to_payment(await self._get_model(payment_id, lock=lock))

    async def set_status(self, payment_id: UUID, status: PaymentStatusEnum) -> Payment:
        row = await self._get_model(payment_id)
        row.status = status
        row.processed_at = row.updated_at = datetime.now(UTC)
        await self.session.flush()
        return to_payment(row)

    async def mark_delivered(self, payment_id: UUID) -> None:
        row = await self._get_model(payment_id)
        row.webhook_delivered_at = datetime.now(UTC)
        await self.session.flush()
