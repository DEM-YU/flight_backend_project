from typing import AsyncGenerator

import pytest_asyncio
import redis.asyncio as aioredis
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

from database import get_db, get_redis, settings
from main import app
from models import Base
import services
from sqlalchemy.ext.asyncio import async_sessionmaker

test_engine = create_async_engine(settings.pg_dsn, poolclass=NullPool)
services.AsyncSessionLocal = async_sessionmaker(bind=test_engine, expire_on_commit=False)


@pytest_asyncio.fixture(autouse=True)
async def ensure_tables():
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@pytest_asyncio.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Provide a real PostgreSQL AsyncSession wrapped in an outer transaction.
    
    Any commits inside the test commit to a savepoint. When the fixture exits,
    the outer transaction is rolled back, leaving PostgreSQL with zero residual data.
    """
    async with test_engine.connect() as conn:
        trans = await conn.begin()
        async with AsyncSession(
            bind=conn,
            expire_on_commit=False,
            join_transaction_mode="create_savepoint",
        ) as session:
            yield session
        await trans.rollback()


@pytest_asyncio.fixture
async def redis_client() -> AsyncGenerator[aioredis.Redis, None]:
    """Provide a real Redis client connected to DB 15 for test isolation."""
    test_url = f"{settings.REDIS_URL.rstrip('/')}/15"
    client = aioredis.Redis.from_url(test_url, decode_responses=True)
    await client.flushdb()
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()


@pytest_asyncio.fixture
async def async_client(db_session: AsyncSession, redis_client: aioredis.Redis) -> AsyncGenerator[AsyncClient, None]:
    """Provide an HTTP async test client with real DB and Redis dependencies wired."""
    app.dependency_overrides[get_db] = lambda: db_session
    app.dependency_overrides[get_redis] = lambda: redis_client
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()
