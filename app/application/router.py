from fastapi import APIRouter, Depends

from app.application.dependencies import require_api_key
from app.application.endpoints.payments import router as payments_router

router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])
router.include_router(payments_router)
