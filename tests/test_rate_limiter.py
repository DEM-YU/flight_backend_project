import uuid
import jwt
import pytest
from fastapi import HTTPException, Request, status
from httpx import ASGITransport, AsyncClient

from database import get_db, get_redis, settings
from main import app
from rate_limiter import RateLimiter


def _build_request(path: str = "/api/v1/orders/reserve", client_ip: str = "127.0.0.1", headers: dict | None = None) -> Request:
    header_list = [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in (headers or {}).items()]
    scope = {
        "type": "http",
        "method": "POST",
        "path": path,
        "headers": header_list,
        "client": (client_ip, 12345),
    }
    return Request(scope)


class TestRateLimiter:
    @pytest.mark.asyncio
    async def test_rate_limiter_allows_under_limit(self, redis_client):
        limiter = RateLimiter(times=5, seconds=10)
        request = _build_request(path="/api/v1/orders/reserve", client_ip="192.168.1.100")

        # 5 consecutive calls must all succeed against real Redis
        for _ in range(5):
            await limiter(request, redis_client)

    @pytest.mark.asyncio
    async def test_rate_limiter_blocks_and_returns_429(self, redis_client):
        limiter = RateLimiter(times=5, seconds=10)
        request = _build_request(path="/api/v1/orders/reserve", client_ip="10.0.0.1")

        # Fill the window limit
        for _ in range(5):
            await limiter(request, redis_client)

        # 6th call must be blocked by Redis Lua script with HTTP 429
        with pytest.raises(HTTPException) as exc_info:
            await limiter(request, redis_client)

        exc = exc_info.value
        assert exc.status_code == status.HTTP_429_TOO_MANY_REQUESTS
        assert int(exc.headers["Retry-After"]) >= 1
        assert exc.headers["X-RateLimit-Limit"] == "5"
        assert exc.headers["X-RateLimit-Remaining"] == "0"

    def test_extract_identifier_authenticated_user(self):
        limiter = RateLimiter(times=5, seconds=10)
        user_id = str(uuid.uuid4())
        token = jwt.encode(
            {"sub": user_id, "type": "access"},
            settings.JWT_SECRET_KEY,
            algorithm=settings.JWT_ALGORITHM,
        )

        request = _build_request(
            path="/api/v1/orders/reserve",
            client_ip="1.2.3.4",
            headers={"Authorization": f"Bearer {token}"},
        )

        ident = limiter._extract_identifier(request)
        assert ident == f"user:{user_id}"

    def test_extract_identifier_anonymous_falls_back_to_ip(self):
        limiter = RateLimiter(times=5, seconds=10)
        request = _build_request(
            path="/api/v1/orders/reserve",
            client_ip="203.0.113.195",
        )

        ident = limiter._extract_identifier(request)
        assert ident == "ip:203.0.113.195"

    def test_extract_identifier_anonymous_ignores_unverified_x_forwarded_for(self):
        limiter = RateLimiter(times=5, seconds=10)
        request = _build_request(
            path="/api/v1/orders/reserve",
            client_ip="10.0.0.1",
            headers={"X-Forwarded-For": "198.51.100.42, 10.0.0.1"},
        )

        ident = limiter._extract_identifier(request)
        assert ident == "ip:10.0.0.1"


class TestRateLimiterIntegration:
    @pytest.mark.asyncio
    async def test_reserve_endpoint_blocked_by_real_rate_limiter(self, async_client):
        user_id = uuid.uuid4()
        token = jwt.encode(
            {"sub": str(user_id), "type": "access"},
            settings.JWT_SECRET_KEY,
            algorithm=settings.JWT_ALGORITHM,
        )
        headers = {"Authorization": f"Bearer {token}"}
        payload = {
            "flight_id": str(uuid.uuid4()),
            "seat_code": "1A",
        }

        limit = settings.RATE_LIMIT_RESERVE_TIMES
        for _ in range(limit):
            resp = await async_client.post("/api/v1/orders/reserve", json=payload, headers=headers)
            assert resp.status_code != 429

        # Request exceeding the limit must receive real HTTP 429
        resp = await async_client.post("/api/v1/orders/reserve", json=payload, headers=headers)
        assert resp.status_code == 429
        assert int(resp.headers["Retry-After"]) >= 1
        assert resp.headers["X-RateLimit-Limit"] == str(limit)
        assert resp.headers["X-RateLimit-Remaining"] == "0"

    @pytest.mark.asyncio
    async def test_untrusted_client_cannot_bypass_rate_limiter_with_spoofed_headers(
        self, db_session, redis_client
    ):
        transport = ASGITransport(app=app, client=("203.0.113.195", 12345))
        app.dependency_overrides[get_db] = lambda: db_session
        app.dependency_overrides[get_redis] = lambda: redis_client

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            limit = settings.RATE_LIMIT_AUTH_TIMES
            for i in range(limit):
                resp = await client.post(
                    "/api/v1/auth/login",
                    json={"email": "nobody@example.com", "password": "wrong"},
                    headers={"X-Forwarded-For": f"198.51.100.{i + 1}"},
                )
                assert resp.status_code == 401

            blocked = await client.post(
                "/api/v1/auth/login",
                json={"email": "nobody@example.com", "password": "wrong"},
                headers={"X-Forwarded-For": "198.51.100.99"},
            )
            assert blocked.status_code == 429
            assert blocked.headers["X-RateLimit-Remaining"] == "0"

    @pytest.mark.asyncio
    async def test_trusted_proxy_correctly_forwards_client_ip(
        self, db_session, redis_client
    ):
        transport = ASGITransport(app=app, client=("127.0.0.1", 12345))
        app.dependency_overrides[get_db] = lambda: db_session
        app.dependency_overrides[get_redis] = lambda: redis_client

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            limit = settings.RATE_LIMIT_AUTH_TIMES
            for _ in range(limit):
                resp = await client.post(
                    "/api/v1/auth/login",
                    json={"email": "nobody@example.com", "password": "wrong"},
                    headers={"X-Forwarded-For": "198.51.100.42"},
                )
                assert resp.status_code == 401

            blocked = await client.post(
                "/api/v1/auth/login",
                json={"email": "nobody@example.com", "password": "wrong"},
                headers={"X-Forwarded-For": "198.51.100.42"},
            )
            assert blocked.status_code == 429

            other_client = await client.post(
                "/api/v1/auth/login",
                json={"email": "nobody@example.com", "password": "wrong"},
                headers={"X-Forwarded-For": "198.51.100.43"},
            )
            assert other_client.status_code == 401
