from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    payment_id: UUID
    attempt: int = Field(default=1, ge=1, le=3)
