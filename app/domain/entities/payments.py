from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue, field_validator

MAX_AMOUNT = Decimal("1000000000.00")
MIN_AMOUNT = Decimal("0.01")
MAX_DESCRIPTION_LEN = 1000
MAX_IDEMPOTENCY_KEY_LEN = 255


class CurrencyEnum(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatusEnum(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class PaymentDetails(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(ge=MIN_AMOUNT, le=MAX_AMOUNT, max_digits=18, decimal_places=2)
    currency: CurrencyEnum
    description: str = Field(min_length=1, max_length=MAX_DESCRIPTION_LEN)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("description")
    @classmethod
    def validate_description(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Payment description must not be empty")
        return value


class Payment(PaymentDetails):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID
    idempotency_key: str
    status: PaymentStatusEnum
    created_at: datetime
    updated_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None
