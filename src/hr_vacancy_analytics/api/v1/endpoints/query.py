from fastapi import APIRouter
from uuid import uuid4

from hr_vacancy_analytics.api.schemas.query import QueryRequest, QueryResponse

router = APIRouter()


@router.post("/query", response_model=QueryResponse)
async def query_endpoint(payload: QueryRequest) -> QueryResponse:
    request_id = str(uuid4())

    # stub-ответ (позже заменим на вызов agent.service)
    answer = f"Received: {payload.text}"

    return QueryResponse(request_id=request_id, answer=answer)
