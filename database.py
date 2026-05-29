from typing import AsyncGenerator

import redis.asyncio as aioredis
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


class Settings(BaseSettings):
    # PostgreSQL
    POSTGRES_USER: str = "flight_user"
    POSTGRES_PASSWORD: str = "flight_pass"
    POSTGRES_SERVER: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "flight_db"

    # Redis
    REDIS_URL: str = "redis://localhost:6379"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def pg_dsn(self) -> str:
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_SERVER}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

settings = Settings()

# SQLAlchemy Async Engine
engine = create_async_engine(
    settings.pg_dsn,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,  # auto-recover stale connections
    echo=False,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
)

# Redis Connection Pool
redis_pool = aioredis.ConnectionPool.from_url(
    settings.REDIS_URL,
    max_connections=50,
    decode_responses=True,
)

# FastAPI Dependencies

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Yields an async PostgreSQL session, auto-closes on exit."""
    async with AsyncSessionLocal() as session:
        yield session


async def get_redis() -> AsyncGenerator[aioredis.Redis, None]:
    """Yields a Redis client from the shared connection pool."""
    client = aioredis.Redis(connection_pool=redis_pool)
    try:
        yield client
    finally:
        await client.aclose()

    
