import os
from urllib.parse import quote, urlsplit

import httpx
import pytest

from app.config import Settings


async def reset_vhost(settings: Settings) -> None:
    url = urlsplit(settings.rabbitmq_url)
    vhost = url.path.lstrip("/")
    if not vhost.endswith("_test"):
        pytest.fail("TEST_RABBITMQ_URL must use a dedicated vhost ending in _test")
    assert url.username is not None and url.password is not None
    management = os.getenv("TEST_RABBITMQ_MANAGEMENT_URL", f"http://{url.hostname}:15672")
    async with httpx.AsyncClient(base_url=management, auth=(url.username, url.password)) as client:
        await configure_vhost(client, quote(vhost, safe=""), quote(url.username, safe=""))


async def configure_vhost(client: httpx.AsyncClient, vhost: str, username: str) -> None:
    response = await client.delete(f"/api/vhosts/{vhost}")
    if response.status_code != 404:
        response.raise_for_status()
    response = await client.put(f"/api/vhosts/{vhost}")
    response.raise_for_status()
    permissions = {"configure": ".*", "write": ".*", "read": ".*"}
    response = await client.put(f"/api/permissions/{vhost}/{username}", json=permissions)
    response.raise_for_status()
