import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import RequestResponseEndpoint
from starlette.responses import Response

from app.application.router import router
from app.infra.base.database import engine


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    yield
    await engine.dispose()


app = FastAPI(title="Payment processing microservice", lifespan=lifespan)


@app.middleware("http")
async def handle_errors(request: Request, call_next: RequestResponseEndpoint) -> Response:
    try:
        return await call_next(request)
    except Exception:
        logging.getLogger(__name__).exception(
            "Unhandled HTTP error: %s %s", request.method, request.url.path
        )
        return JSONResponse(
            {"detail": "Sorry, something wrong, please try again later!"}, status_code=500
        )


app.include_router(router)
