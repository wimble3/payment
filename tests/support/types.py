from aio_pika.abc import AbstractChannel
from faststream.rabbit import RabbitBroker
from pydantic import JsonValue

type JsonObject = dict[str, JsonValue]
type RabbitSession = tuple[RabbitBroker, AbstractChannel]
type ReceivedWebhook = tuple[JsonObject, dict[str, str], float]
