import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select

from app.infra.base.database import SessionFactory
from app.infra.models import OutboxModel, PaymentModel
from app.infra.repositories.outbox import SQLEventRepository
from tests.support.types import JsonObject

pytestmark = pytest.mark.e2e


async def assert_counts(database: SessionFactory, payments: int = 1, outbox: int = 1) -> None:
    async with database() as session:
        assert await session.scalar(select(func.count()).select_from(PaymentModel)) == payments
        assert await session.scalar(select(func.count()).select_from(OutboxModel)) == outbox


async def assert_payment_details(
    client: httpx.AsyncClient, accepted: JsonObject, payment_body: JsonObject
) -> None:
    payment_id = accepted["payment_id"]
    detail = await client.get(f"/api/v1/payments/{payment_id}")
    assert detail.status_code == 200
    assert detail.json()["amount"] == "125.50"
    assert detail.json()["metadata"] == payment_body["metadata"]
    assert detail.json()["processed_at"] is None


async def test_create_and_get(
    client: httpx.AsyncClient, payment_body: JsonObject, database: SessionFactory
) -> None:
    response = await client.post(
        "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "order-42"}
    )
    assert response.status_code == 202, response.text
    accepted = response.json()
    assert accepted["status"] == "pending"
    assert set(accepted) == {"payment_id", "status", "created_at"}
    await assert_payment_details(client, accepted, payment_body)
    await assert_counts(database)
    async with database() as session:
        assert (await session.scalars(select(OutboxModel))).one().published_at is None


async def assert_equivalent_and_conflicting_requests(
    client: httpx.AsyncClient, payment_body: JsonObject
) -> None:
    equivalent = dict(payment_body, amount="125.500")
    response = await client.post(
        "/api/v1/payments", json=equivalent, headers={"Idempotency-Key": "same-key"}
    )
    assert response.status_code == 202
    conflict = await client.post(
        "/api/v1/payments",
        json=dict(payment_body, amount="126"),
        headers={"Idempotency-Key": "same-key"},
    )
    assert conflict.status_code == 409


async def test_concurrent_idempotency(
    client: httpx.AsyncClient, payment_body: JsonObject, database: SessionFactory
) -> None:
    requests = [
        client.post("/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "same-key"})
        for _ in range(10)
    ]
    responses = await asyncio.gather(*requests)
    assert {response.status_code for response in responses} == {202}
    assert len({response.json()["payment_id"] for response in responses}) == 1
    await assert_equivalent_and_conflicting_requests(client, payment_body)
    await assert_counts(database)


@pytest.mark.parametrize(
    "method,path", [("POST", "/api/v1/payments"), ("GET", f"/api/v1/payments/{uuid4()}")]
)
@pytest.mark.parametrize("key", [None, "", "wrong"])
async def test_authentication(
    client: httpx.AsyncClient, method: str, path: str, key: str | None
) -> None:
    request = client.build_request(method, path)
    request.headers.pop("X-API-Key", None)
    if key is not None:
        request.headers["X-API-Key"] = key
    response = await client.send(request)
    assert response.status_code == 401


@pytest.mark.parametrize("path", ["/docs", "/redoc", "/openapi.json"])
async def test_public_documentation(client: httpx.AsyncClient, path: str) -> None:
    request = client.build_request("GET", path)
    request.headers.pop("X-API-Key", None)
    response = await client.send(request)
    assert response.status_code == 200


async def test_swagger_authentication(client: httpx.AsyncClient) -> None:
    schema = (await client.get("/openapi.json")).json()
    assert schema["components"]["securitySchemes"]["APIKeyHeader"] == {
        "type": "apiKey",
        "in": "header",
        "name": "X-API-Key",
    }
    for path, method in [("/api/v1/payments", "post"), ("/api/v1/payments/{payment_id}", "get")]:
        assert schema["paths"][path][method]["security"] == [{"APIKeyHeader": []}]


@pytest.mark.parametrize(
    "change",
    [
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "NaN"},
        {"amount": "Infinity"},
        {"amount": "1.001"},
        {"amount": "1000000000.01"},
        {"currency": "GBP"},
        {"description": "  "},
        {"webhook_url": "ftp://example.com"},
        {"payment_id": str(uuid4())},
    ],
)
async def test_invalid_body(
    client: httpx.AsyncClient, payment_body: JsonObject, change: JsonObject
) -> None:
    response = await client.post(
        "/api/v1/payments",
        json=dict(payment_body, **change),
        headers={"Idempotency-Key": "valid-key"},
    )
    assert response.status_code == 422, response.text


async def test_invalid_key_and_not_found(
    client: httpx.AsyncClient, payment_body: JsonObject
) -> None:
    for headers in ({}, {"Idempotency-Key": " "}, {"Idempotency-Key": "a" * 256}):
        response = await client.post("/api/v1/payments", json=payment_body, headers=headers)
        assert response.status_code == 422
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404
    assert (await client.get("/api/v1/payments/not-a-uuid")).status_code == 422


async def fail_outbox(*args: object) -> None:
    raise RuntimeError("outbox write failed")


async def test_creation_rolls_back_with_outbox_failure(
    client: httpx.AsyncClient,
    payment_body: JsonObject,
    monkeypatch: pytest.MonkeyPatch,
    database: SessionFactory,
) -> None:
    monkeypatch.setattr(SQLEventRepository, "add", fail_outbox)
    response = await client.post(
        "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "rollback"}
    )
    assert_unexpected_error(response)
    async with database() as session:
        assert await session.scalar(select(func.count()).select_from(PaymentModel)) == 0


def assert_unexpected_error(response: httpx.Response) -> None:
    assert response.status_code == 500
    assert response.json() == {"detail": "Sorry, something wrong, please try again later!"}


@pytest.mark.parametrize(
    "method,path,target",
    [
        ("POST", "/api/v1/payments", "app.domain.use_cases.payments.CreatePaymentUseCase.execute"),
        (
            "GET",
            f"/api/v1/payments/{uuid4()}",
            "app.domain.use_cases.payments.GetPaymentUseCase.execute",
        ),
    ],
)
@pytest.mark.parametrize("error_type", [RuntimeError, ValueError, KeyError])
async def test_unhandled_view_error(
    client: httpx.AsyncClient,
    payment_body: JsonObject,
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    path: str,
    target: str,
    error_type: type[Exception],
) -> None:
    monkeypatch.setattr(target, AsyncMock(side_effect=error_type("Sensitive internal details")))
    response = await client.request(
        method, path, json=payment_body, headers={"Idempotency-Key": "middleware-error"}
    )
    assert_unexpected_error(response)
