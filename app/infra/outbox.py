import asyncio
import logging
from datetime import UTC, datetime, timedelta

from aio_pika.exceptions import (
    ChannelClosed,
    ChannelInvalidStateError,
    DeliveryError,
)
from faststream.rabbit import Channel, RabbitBroker
from sqlalchemy import Select, func, select
from sqlalchemy.exc import DBAPIError, DisconnectionError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from app.config import Settings, get_settings
from app.infra.base.database import SessionFactory, engine, session_factory
from app.infra.models import OutboxModel
from app.transport.faststream.topology import declare_topology, make_routes

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self, broker: RabbitBroker, settings: Settings, factory: SessionFactory = session_factory
    ) -> None:
        self.broker, self.settings, self.factory = broker, settings, factory
        self.routes = make_routes(settings)

    def _pending_query(self) -> Select[OutboxModel]:
        return (
            select(OutboxModel)
            .where(OutboxModel.published_at.is_(None), OutboxModel.available_at <= func.now())
            .order_by(OutboxModel.created_at, OutboxModel.id)
            .limit(self.settings.outbox_batch_size)
            .with_for_update(skip_locked=True)
        )

    async def publish_batch(self) -> int:
        async with self.factory.begin() as session:
            rows = (await session.scalars(self._pending_query())).all()
            for row in rows:
                await self._publish_event(row)
            return len(rows)

    async def _publish_event(self, row: OutboxModel) -> None:
        row.publish_attempts += 1
        try:
            await self._send(row)
        except (
            ChannelClosed,
            ChannelInvalidStateError,
            DeliveryError,
            ConnectionError,
            TimeoutError,
        ) as exc:
            self._record_failure(row, exc)
        else:
            row.published_at = datetime.now(UTC)
            row.last_error = None

    async def _send(self, row: OutboxModel) -> None:
        route = self.routes[row.topic]
        await self.broker.publish(
            row.payload,
            queue=route.queue,
            exchange=route.exchange,
            persist=True,
            mandatory=True,
            timeout=10,
            message_id=str(row.id),
        )

    def _record_failure(self, row: OutboxModel, error: Exception) -> None:
        logger.exception("Outbox publish failed: id=%s topic=%s", row.id, row.topic)
        row.last_error = type(error).__name__
        delay = min(60, 2 ** min(row.publish_attempts, 6))
        row.available_at = datetime.now(UTC) + timedelta(seconds=delay)

    async def run(self) -> None:
        while True:
            try:
                count = await self.publish_batch()
            except (
                DBAPIError,
                DisconnectionError,
                PoolTimeoutError,
                ConnectionError,
                TimeoutError,
            ):
                logger.exception("Outbox polling failed")
                count = 0
            if count < self.settings.outbox_batch_size:
                await asyncio.sleep(self.settings.outbox_poll_interval)


async def main() -> None:
    settings = get_settings()
    channel = Channel(publisher_confirms=True, on_return_raises=True)
    broker = RabbitBroker(settings.rabbitmq_url, default_channel=channel)
    try:
        async with broker:
            relay = OutboxRelay(broker, settings)
            await declare_topology(broker, relay.routes)
            await relay.run()
    finally:
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
