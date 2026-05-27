from contextlib import asynccontextmanager
from typing import Annotated

import redis.asyncio as aioredis
from fastapi import Depends, FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api import router
from database import engine, get_db, get_redis
from models import Base


# Lifespan

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()

# app

app = FastAPI(
    title="Flight Booking API",
    description="High-concurrency transatlantic flight booking system.",
    version="0.3.0",
    lifespan=lifespan,
)

#  Routes 

@app.get("/health", tags=["Infrastructure"])
async def health_check(
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
) -> dict:
    """
    Liveness & readiness probe.
    Verifies connectivity to both PostgreSQL and Redis.
    """
    status = {"status": "ok", "postgres": "unknown", "redis": "unknown"}

    try:
        await db.execute(text("SELECT 1"))
        status["postgres"] = "healthy"
    except Exception as e:
        status["postgres"] = f"unhealthy: {e}"
        status["status"] = "degraded"

    try:
        pong = await redis.ping()
        status["redis"] = "healthy" if pong else "unhealthy"
    except Exception as e:
        status["redis"] = f"unhealthy: {e}"
        status["status"] = "degraded"

    return status


app.include_router(router)