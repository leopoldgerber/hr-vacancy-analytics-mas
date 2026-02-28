from fastapi import FastAPI
from datetime import datetime

from hr_vacancy_analytics.api.v1.router import router as v1_router


def create_app() -> FastAPI:
    app = FastAPI(
        title="HR Vacancy Analytics API",
        description="API service with LLM Agent for HR vacancy analytics",
        version="0.1.0",
    )

    app.include_router(v1_router)

    @app.get("/health", tags=["health"])
    async def health_check():
        return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}

    return app


app = create_app()
