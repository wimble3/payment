from datetime import datetime
from uuid import UUID, uuid4

from pydantic import JsonValue
from sqlalchemy import DateTime, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.infra.base.database import Base


class OutboxModel(Base):
    __tablename__ = "outbox"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    topic: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text)


Index(
    "ix_outbox_unpublished",
    OutboxModel.available_at,
    OutboxModel.created_at,
    postgresql_where=OutboxModel.published_at.is_(None),
)
