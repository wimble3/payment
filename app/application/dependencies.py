import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, Security
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.infra.base.database import session_factory

api_key_header: APIKeyHeader = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_api_key(provided: Annotated[str | None, Security(api_key_header)]) -> None:
    expected = get_settings().api_key.get_secret_value()
    if provided is None or not secrets.compare_digest(provided.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="Invalid API key")


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    return session_factory


SessionFactory = Annotated[async_sessionmaker[AsyncSession], Depends(get_session_factory)]
