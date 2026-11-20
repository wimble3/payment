from dataclasses import dataclass

from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.schemas import QueueType
from faststream.rabbit.schemas.queue import QuorumQueueArgs

from app.config import Settings

TOPICS = ("payments.new", "payments.status")


@dataclass(frozen=True)
class Route:
    queue: RabbitQueue
    exchange: RabbitExchange | None = None


def dead_letter_arguments(topic: str, exchange: str) -> QuorumQueueArgs:
    return {
        "x-dead-letter-exchange": exchange,
        "x-dead-letter-routing-key": topic,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    }


def main_route(topic: str) -> Route:
    arguments = dead_letter_arguments(topic, f"{topic}.dead.dlx")
    arguments["x-delivery-limit"] = -1
    return Route(RabbitQueue(topic, durable=True, queue_type=QueueType.QUORUM, arguments=arguments))


def dead_route(topic: str) -> Route:
    exchange = RabbitExchange(f"{topic}.dead.dlx", durable=True)
    queue = RabbitQueue(
        f"{topic}.dead.dlq", durable=True, queue_type=QueueType.QUORUM, routing_key=topic
    )
    return Route(queue, exchange)


def retry_route(topic: str, attempt: int, settings: Settings) -> Route:
    name = f"{topic}.retry.dlq" if attempt == 1 else f"{topic}.retry.2.dlq"
    arguments = dead_letter_arguments(topic, "")
    arguments["x-message-ttl"] = max(
        1, int(settings.retry_base_seconds * 1000 * 2 ** (attempt - 1))
    )
    queue = RabbitQueue(
        name,
        durable=True,
        queue_type=QueueType.QUORUM,
        routing_key=f"{topic}.{attempt}",
        arguments=arguments,
    )
    return Route(queue, RabbitExchange(f"{topic}.retry.dlx", durable=True))


def make_routes(settings: Settings) -> dict[str, Route]:
    routes = {}
    for topic in TOPICS:
        routes[topic] = main_route(topic)
        routes[f"{topic}.dead"] = dead_route(topic)
        for attempt in (1, 2):
            routes[f"{topic}.retry.{attempt}"] = retry_route(topic, attempt, settings)
    return routes


async def declare_topology(broker: RabbitBroker, routes: dict[str, Route]) -> None:
    for route in routes.values():
        if route.exchange is not None:
            exchange = await broker.declare_exchange(route.exchange)
            queue = await broker.declare_queue(route.queue)
            await queue.bind(exchange, routing_key=route.queue.routing())
        else:
            await broker.declare_queue(route.queue)
