import asyncio
import hashlib
import logging
from functools import partial
from json import JSONDecodeError
from typing import cast

import httpx
from faststream import AckPolicy, FastStream
from faststream.message import decode_message
from faststream.rabbit import Channel, RabbitBroker, RabbitQueue
from faststream.rabbit.annotations import RabbitMessage
from faststream.rabbit.message import RabbitMessage as IncomingMessage
from pydantic import JsonValue

from app.config import Settings, get_settings
from app.domain.entities.events import PaymentEvent
from app.domain.ports import PaymentGateway, WebhookGateway
from app.domain.use_cases.base import IUseCase
from app.domain.use_cases.messages import HandlePaymentMessageUseCase, SchedulePaymentFailureUseCase
from app.domain.use_cases.payments import ProcessPaymentUseCase, SendPaymentWebhookUseCase
from app.infra.base.database import SessionFactory, engine
from app.infra.base.database import session_factory as default_session_factory
from app.infra.gateways.payments import MockPaymentGateway
from app.infra.gateways.webhooks import HTTPWebhookGateway
from app.transport.faststream.topology import Route, declare_topology, main_route, make_routes

logger: logging.Logger = logging.getLogger(__name__)


def decode_payment_message(message: IncomingMessage) -> JsonValue:
    try:
        body = decode_message(message)
    except (JSONDecodeError, UnicodeDecodeError):
        return message.body.decode(errors="replace")
    return body.decode(errors="replace") if isinstance(body, bytes) else cast(JsonValue, body)


def create_broker(settings: Settings) -> RabbitBroker:
    channel = Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True)
    return RabbitBroker(
        settings.rabbitmq_url, decoder=decode_payment_message, default_channel=channel
    )


async def run_use_case(
    case: HandlePaymentMessageUseCase,
    failures: SchedulePaymentFailureUseCase,
    body: JsonValue,
    message_id: str,
) -> None:
    try:
        await case.execute(body)
    except Exception as error:
        logger.exception("Payment message failed: %s", message_id)
        await failures.execute(body, message_id, error)


async def consume_message(
    case: HandlePaymentMessageUseCase,
    failures: SchedulePaymentFailureUseCase,
    body: JsonValue,
    message: RabbitMessage,
) -> None:
    message_id = message.message_id or hashlib.sha256(message.body).hexdigest()
    try:
        await run_use_case(case, failures, body, message_id)
    except Exception:
        logger.exception("Cannot persist delivery outcome; requeuing")
        await asyncio.sleep(0.5)
        await message.nack(requeue=True)
    else:
        await message.ack()


def subscribe(
    broker: RabbitBroker,
    queue: RabbitQueue,
    case: HandlePaymentMessageUseCase,
    failures: SchedulePaymentFailureUseCase,
) -> None:
    @broker.subscriber(queue, ack_policy=AckPolicy.MANUAL)
    async def handle(body: JsonValue, message: RabbitMessage) -> None:
        await consume_message(case, failures, body, message)


def register_use_cases(
    broker: RabbitBroker, factory: SessionFactory, gateway: PaymentGateway, webhook: WebhookGateway
) -> None:
    cases: tuple[tuple[str, IUseCase[[PaymentEvent], None]], ...] = (
        ("payments.new", ProcessPaymentUseCase(factory, gateway)),
        ("payments.status", SendPaymentWebhookUseCase(factory, webhook)),
    )
    for topic, handler in cases:
        case = HandlePaymentMessageUseCase(factory, handler, topic)
        failures = SchedulePaymentFailureUseCase(factory, topic)
        subscribe(broker, main_route(topic).queue, case, failures)


async def setup(broker: RabbitBroker, routes: dict[str, Route]) -> None:
    await broker.connect()
    await declare_topology(broker, routes)


async def shutdown(client: httpx.AsyncClient) -> None:
    await client.aclose()
    await engine.dispose()


def create_consumer_app(
    settings: Settings,
    session_factory: SessionFactory = default_session_factory,
    payment_gateway: PaymentGateway | None = None,
    webhook_gateway: WebhookGateway | None = None,
) -> FastStream:
    broker = create_broker(settings)
    client = httpx.AsyncClient(timeout=settings.webhook_timeout, follow_redirects=False)
    application = FastStream(broker)
    gateway = payment_gateway or MockPaymentGateway(settings)
    webhook = webhook_gateway or HTTPWebhookGateway(client)
    register_use_cases(broker, session_factory, gateway, webhook)
    application.on_startup(partial(setup, broker, make_routes(settings)))
    application.after_shutdown(partial(shutdown, client))
    return application


app: FastStream = create_consumer_app(get_settings())
