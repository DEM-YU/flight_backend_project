import logging
from contextlib import asynccontextmanager
from typing import Annotated

import redis.asyncio as aioredis
from fastapi import Depends, FastAPI, Request, Response
from fastapi.responses import JSONResponse
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from api import router
from database import engine, get_db, get_redis
from models import Base

logger = logging.getLogger(__name__)


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
    version="0.4.0",
    lifespan=lifespan,
)


# ── Global Exception Handlers (CR-06) ──

@app.exception_handler(OperationalError)
async def db_exception_handler(request: Request, exc: OperationalError) -> JSONResponse:
    logger.error("Database operational error on %s: %s", request.url.path, exc, exc_info=True)
    return JSONResponse(
        status_code=503,
        content={"detail": "Database temporarily unavailable. Please retry."},
    )


@app.exception_handler(RedisConnectionError)
async def redis_exception_handler(request: Request, exc: RedisConnectionError) -> JSONResponse:
    logger.error("Redis connection error on %s: %s", request.url.path, exc, exc_info=True)
    return JSONResponse(
        status_code=503,
        content={"detail": "Service temporarily unavailable. Please retry."},
    )


#  Routes

@app.get("/health", tags=["Infrastructure"])
async def health_check(
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
    response: Response,
) -> dict:
    """
    Liveness & readiness probe.
    Verifies connectivity to both PostgreSQL and Redis.
    Returns HTTP 503 when any dependency is degraded.
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

    if status["status"] != "ok":
        response.status_code = 503

    return status


app.include_router(router)