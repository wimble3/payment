from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from app.domain.entities.payments import PaymentDetails, PaymentStatusEnum


class PaymentCreateSchema(PaymentDetails):
    pass


class PaymentGetSchema(BaseModel):
    payment_id: UUID


class PaymentAcceptedSchema(BaseModel):
    payment_id: UUID
    status: PaymentStatusEnum
    created_at: datetime
