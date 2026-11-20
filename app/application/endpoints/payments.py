from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException

from app.application.dependencies import SessionFactory
from app.application.schemas.payments import PaymentAcceptedSchema, PaymentCreateSchema
from app.domain.entities.payments import Payment
from app.domain.exceptions import IdempotencyConflict
from app.domain.use_cases.payments import CreatePaymentUseCase, GetPaymentUseCase
from app.infra.base.exceptions import DBNotFound

router = APIRouter(prefix="/payments", tags=["payments"])
IdempotencyKey = Annotated[
    str, Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"^[\x21-\x7E]+$")
]


@router.post("", response_model=PaymentAcceptedSchema, status_code=202)
async def create_payment(
    payment_create_schema: PaymentCreateSchema,
    idempotency_key: IdempotencyKey,
    session_factory: SessionFactory,
) -> Payment:
    use_case = CreatePaymentUseCase(session_factory)
    try:
        return await use_case.execute(payment_create_schema, idempotency_key)
    except IdempotencyConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/{payment_id}", response_model=Payment)
async def get_payment(payment_id: UUID, session_factory: SessionFactory) -> Payment:
    use_case = GetPaymentUseCase(session_factory)
    try:
        return await use_case.execute(payment_id)
    except DBNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
