from pydantic import BaseModel, Field
from typing import Optional


class QueryRequest(BaseModel):
    text: str = Field(
        ...,
        min_length=1,
        description="User question in RU or EN",
        examples=[
            "Which profile generated more responses in the city of Moscow: "
            "Assistant Store Manager or Sales Assistant?",
            "Какой профиль принес больше откликов по городу Москва: "
            "Assistant Store Manager или Sales Assistant?"
        ],
    )


class QueryResponse(BaseModel):
    request_id: str = Field(..., description="Unique request identifier")
    answer: str = Field(..., description="Final answer returned by the agent")
    metadata: Optional[dict] = Field(
        default=None,
        description="Optional metadata (city, profiles, metric, etc.)",
    )
