import asyncio
import os
import subprocess
from collections.abc import AsyncIterator
from uuid import uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.application.dependencies import get_session_factory
from app.config import Settings, get_settings
from app.infra.base.database import SessionFactory
from app.infra.models import OutboxModel, PaymentModel
from app.main import app
from tests.support.types import JsonObject


@pytest.fixture
def settings() -> Settings:
    database = os.getenv(
        "TEST_DATABASE_URL", "postgresql+asyncpg://payment:payment@localhost:5432/payment_test"
    )
    rabbit = os.getenv("TEST_RABBITMQ_URL", "amqp://guest:guest@localhost:5672/payment_test")
    return Settings(
        database_url=database,
        rabbitmq_url=rabbit,
        gateway_min_seconds=0,
        gateway_max_seconds=0,
        gateway_success_rate=1,
        retry_base_seconds=0.05,
        outbox_poll_interval=0.01,
    )


def database_identifier(url: URL) -> str:
    assert url.database is not None
    quote = chr(34)
    return quote + url.database.replace(quote, quote * 2) + quote


async def create_database(url: URL) -> None:
    admin = url.set(drivername="postgresql", database="postgres").render_as_string(
        hide_password=False
    )
    connection = await asyncpg.connect(admin)
    try:
        exists = await connection.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", url.database
        )
        if not exists:
            name = database_identifier(url)
            await connection.execute(f"CREATE DATABASE {name}")
    finally:
        await connection.close()


async def prepare_database(settings: Settings) -> None:
    url = make_url(settings.database_url)
    if not url.database or not url.database.endswith("_test"):
        pytest.fail("TEST_DATABASE_URL must point to a dedicated database ending in _test")
    await create_database(url)
    env = dict(os.environ, DATABASE_URL=settings.database_url)
    await asyncio.to_thread(
        subprocess.run, ["alembic", "upgrade", "head"], env=env, check=True, capture_output=True
    )


async def clear_database(factory: SessionFactory) -> None:
    async with factory.begin() as session:
        events = (await session.scalars(select(OutboxModel))).all()
        payments = (await session.scalars(select(PaymentModel))).all()
        for row in (*events, *payments):
            await session.delete(row)


@pytest_asyncio.fixture
async def database(settings: Settings) -> AsyncIterator[SessionFactory]:
    await prepare_database(settings)
    engine = create_async_engine(settings.database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    await clear_database(factory)
    try:
        yield factory
    finally:
        await engine.dispose()


@pytest.fixture
def session_factory(database: SessionFactory) -> SessionFactory:
    return database


@pytest_asyncio.fixture
async def client(session_factory: SessionFactory) -> AsyncIterator[httpx.AsyncClient]:
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    headers = {"X-API-Key": get_settings().api_key.get_secret_value()}
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://api", headers=headers
        ) as client:
            yield client
    finally:
        app.dependency_overrides.clear()


@pytest.fixture
def payment_body() -> JsonObject:
    return {
        "amount": "125.50",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": str(uuid4())},
        "webhook_url": "http://127.0.0.1:9999/webhook",
    }
