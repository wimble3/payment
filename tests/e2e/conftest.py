from collections.abc import AsyncIterator

import aio_pika
import httpx
import pytest
import pytest_asyncio
from faststream.rabbit import Channel, RabbitBroker

from app.config import Settings
from app.infra.base.database import SessionFactory
from app.transport.faststream.topology import declare_topology, make_routes
from tests.support.pipeline import Pipeline
from tests.support.rabbit import reset_vhost
from tests.support.types import JsonObject, RabbitSession


@pytest_asyncio.fixture
async def rabbit(settings: Settings) -> AsyncIterator[RabbitSession]:
    await reset_vhost(settings)
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    broker = RabbitBroker(settings.rabbitmq_url, default_channel=Channel(on_return_raises=True))
    try:
        async with broker:
            await declare_topology(broker, make_routes(settings))
            yield broker, await connection.channel()
    finally:
        await connection.close()


@pytest.fixture
def pipeline(
    settings: Settings,
    database: SessionFactory,
    client: httpx.AsyncClient,
    payment_body: JsonObject,
    rabbit: RabbitSession,
) -> Pipeline:
    return Pipeline(settings, database, client, payment_body, rabbit)
