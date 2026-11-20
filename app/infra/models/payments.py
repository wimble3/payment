from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import JsonValue
from sqlalchemy import CheckConstraint, DateTime, Numeric, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.infra.base.database import Base


class PaymentModel(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount >= 0.01 AND amount <= 1000000000", name="ck_payment_amount"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payment_currency"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="ck_payment_status"),
    )

    payment_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(String(1000))
    metadata_json: Mapped[dict[str, JsonValue]] = mapped_column("metadata", JSONB)
    webhook_url: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(16), default="pending")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
