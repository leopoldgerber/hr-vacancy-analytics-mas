from fastapi import APIRouter

from hr_vacancy_analytics.api.v1.endpoints.query import router as query_router

router = APIRouter(prefix="/api/v1")
router.include_router(query_router, tags=["query"])
