from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import JsonValue, ValidationError

from app.domain.entities.events import PaymentEvent
from app.domain.use_cases.base import IUseCase
from app.infra.base.database import SessionFactory
from app.infra.repositories.outbox import SQLEventRepository


class HandlePaymentMessageUseCase(IUseCase[[JsonValue], None]):
    def __init__(
        self, session_factory: SessionFactory, handler: IUseCase[[PaymentEvent], None], topic: str
    ) -> None:
        self.session_factory, self.handler, self.topic = session_factory, handler, topic

    async def execute(self, body: JsonValue) -> None:
        event = PaymentEvent.model_validate(body)
        failure_id = uuid5(event.event_id, f"{self.topic}:{event.attempt}:failure")
        async with self.session_factory.begin() as session:
            if await SQLEventRepository(session).exists(failure_id):
                return
        await self.handler.execute(event)


class SchedulePaymentFailureUseCase(IUseCase[[JsonValue, str, Exception], None]):
    def __init__(self, session_factory: SessionFactory, topic: str) -> None:
        self.session_factory, self.topic = session_factory, topic

    async def execute(self, body: JsonValue, message_id: str, error: Exception) -> None:
        try:
            event = PaymentEvent.model_validate(body)
        except ValidationError:
            await self._schedule_invalid(body, message_id)
            return
        failure_id = uuid5(event.event_id, f"{self.topic}:{event.attempt}:failure")
        await self._schedule_failure(event, failure_id, error)

    async def _schedule_failure(
        self, event: PaymentEvent, failure_id: UUID, error: Exception
    ) -> None:
        payload = event.model_dump(mode="json")
        if event.attempt < 3:
            payload["attempt"] += 1
            destination = f"{self.topic}.retry.{event.attempt}"
        else:
            destination = f"{self.topic}.dead"
            payload["error"] = type(error).__name__
        await self._schedule(failure_id, destination, payload)

    async def _schedule_invalid(self, body: JsonValue, message_id: str) -> None:
        event_id = uuid5(NAMESPACE_URL, f"{self.topic}:invalid:{message_id}")
        payload: dict[str, JsonValue] = {"original": body, "error": "Invalid payment event"}
        await self._schedule(event_id, f"{self.topic}.dead", payload)

    async def _schedule(self, event_id: UUID, topic: str, payload: dict[str, JsonValue]) -> None:
        async with self.session_factory.begin() as session:
            await SQLEventRepository(session).add(event_id, topic, payload)
