import uuid
from unittest.mock import AsyncMock, MagicMock

import jwt
import pytest
from fastapi import HTTPException, Request, status
from httpx import ASGITransport, AsyncClient

from database import get_db, get_redis, settings
from main import app
from rate_limiter import RateLimiter, SLIDING_WINDOW_SCRIPT

pytestmark = pytest.mark.asyncio


class TestRateLimiterUnit:
    async def test_rate_limiter_allows_request_when_under_limit(self):
        redis = AsyncMock()
        redis.eval.return_value = [1, 4, 0]

        limiter = RateLimiter(times=5, seconds=10)
        request = MagicMock(spec=Request)
        request.headers = {}
        request.client = MagicMock(host="192.168.1.100")
        request.url = MagicMock(path="/api/v1/orders/reserve")

        await limiter(request, redis)

        redis.eval.assert_called_once()
        args = redis.eval.call_args.args
        assert args[0] == SLIDING_WINDOW_SCRIPT
        assert args[1] == 1
        assert args[2] == "rate_limit:/api/v1/orders/reserve:ip:192.168.1.100"
        assert args[4] == 10
        assert args[5] == 5

    async def test_rate_limiter_blocks_when_limit_exceeded(self):
        redis = AsyncMock()
        redis.eval.return_value = [0, 0, 6]

        limiter = RateLimiter(times=5, seconds=10)
        request = MagicMock(spec=Request)
        request.headers = {}
        request.client = MagicMock(host="10.0.0.1")
        request.url = MagicMock(path="/api/v1/orders/reserve")

        with pytest.raises(HTTPException) as exc_info:
            await limiter(request, redis)

        exc = exc_info.value
        assert exc.status_code == status.HTTP_429_TOO_MANY_REQUESTS
        assert exc.headers["Retry-After"] == "6"
        assert exc.headers["X-RateLimit-Limit"] == "5"
        assert exc.headers["X-RateLimit-Remaining"] == "0"

    async def test_extract_identifier_authenticated_user(self):
        limiter = RateLimiter(times=5, seconds=10)
        user_id = str(uuid.uuid4())
        token = jwt.encode(
            {"sub": user_id, "type": "access"},
            settings.JWT_SECRET_KEY,
            algorithm=settings.JWT_ALGORITHM,
        )

        request = MagicMock(spec=Request)
        request.headers = {"Authorization": f"Bearer {token}"}
        request.client = MagicMock(host="1.2.3.4")

        ident = limiter._extract_identifier(request)
        assert ident == f"user:{user_id}"

    async def test_extract_identifier_anonymous_falls_back_to_ip(self):
        limiter = RateLimiter(times=5, seconds=10)
        request = MagicMock(spec=Request)
        request.headers = {}
        request.client = MagicMock(host="203.0.113.195")

        ident = limiter._extract_identifier(request)
        assert ident == "ip:203.0.113.195"


class TestRateLimiterIntegration:
    async def test_reserve_endpoint_blocked_by_rate_limiter(self):
        redis_mock = AsyncMock()
        redis_mock.eval.return_value = [0, 0, 9]

        db_mock = MagicMock()
        user_id = uuid.uuid4()
        token = jwt.encode(
            {"sub": str(user_id), "type": "access"},
            settings.JWT_SECRET_KEY,
            algorithm=settings.JWT_ALGORITHM,
        )

        app.dependency_overrides[get_redis] = lambda: redis_mock
        app.dependency_overrides[get_db] = lambda: db_mock

        try:
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
                headers = {"Authorization": f"Bearer {token}"}
                payload = {
                    "flight_id": str(uuid.uuid4()),
                    "seat_code": "1A",
                }
                resp = await ac.post("/api/v1/orders/reserve", json=payload, headers=headers)

                assert resp.status_code == 429
                assert resp.headers["Retry-After"] == "9"
                assert resp.headers["X-RateLimit-Limit"] == str(settings.RATE_LIMIT_RESERVE_TIMES)
                assert resp.headers["X-RateLimit-Remaining"] == "0"
        finally:
            app.dependency_overrides.clear()
