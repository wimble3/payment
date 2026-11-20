from uuid import UUID

from pydantic import JsonValue
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.infra.base.exceptions import is_unique_violation
from app.infra.models import OutboxModel


class SQLEventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add(self, event_id: UUID, topic: str, payload: dict[str, JsonValue]) -> None:
        try:
            async with self.session.begin_nested():
                self.session.add(OutboxModel(id=event_id, topic=topic, payload=payload))
                await self.session.flush()
        except IntegrityError as error:
            if not is_unique_violation(error, "outbox_pkey"):
                raise

    async def exists(self, event_id: UUID) -> bool:
        return await self.session.get(OutboxModel, event_id) is not None
