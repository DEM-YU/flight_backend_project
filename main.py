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
from database import engine, get_db, get_redis, redis_pool
from models import Base

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create tables on startup, dispose engine and Redis pool on shutdown."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()
    await redis_pool.aclose()



app = FastAPI(
    title="Flight Booking API",
    description="High-concurrency transatlantic flight booking system.",
    version="0.4.0",
    lifespan=lifespan,
)


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


@app.get("/health", tags=["Infrastructure"])
async def health_check(
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
    response: Response,
) -> dict:
    health_report = {"status": "ok", "postgres": "unknown", "redis": "unknown"}

    try:
        await db.execute(text("SELECT 1"))
        health_report["postgres"] = "healthy"
    except Exception as e:
        health_report["postgres"] = f"unhealthy: {e}"
        health_report["status"] = "degraded"

    try:
        pong = await redis.ping()
        health_report["redis"] = "healthy" if pong else "unhealthy"
    except Exception as e:
        health_report["redis"] = f"unhealthy: {e}"
        health_report["status"] = "degraded"

    if health_report["status"] != "ok":
        response.status_code = 503

    return health_report


app.include_router(router)
