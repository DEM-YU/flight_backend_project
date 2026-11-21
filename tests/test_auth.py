import uuid
import jwt
import pytest
from fastapi import HTTPException, status
from sqlalchemy import select

from auth import (
    create_access_token,
    create_refresh_token,
    get_current_user_id,
    hash_password,
    verify_password,
)
from database import settings
from models import User


def test_password_hashing():
    password = "secret_password"
    hashed = hash_password(password)
    assert hashed != password
    assert verify_password(password, hashed) is True
    assert verify_password("wrong_password", hashed) is False


def test_token_creation_and_validation():
    user_id = uuid.uuid4()
    access_token = create_access_token(user_id)
    refresh_token = create_refresh_token(user_id)

    payload = jwt.decode(access_token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
    assert payload["sub"] == str(user_id)
    assert payload["type"] == "access"

    payload_refresh = jwt.decode(refresh_token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
    assert payload_refresh["sub"] == str(user_id)
    assert payload_refresh["type"] == "refresh"


@pytest.mark.asyncio
async def test_get_current_user_id_valid():
    user_id = uuid.uuid4()
    token = create_access_token(user_id)
    result = await get_current_user_id(token)
    assert result == user_id


@pytest.mark.asyncio
async def test_get_current_user_id_invalid():
    with pytest.raises(HTTPException) as exc:
        await get_current_user_id("invalid-token")
    assert exc.value.status_code == status.HTTP_401_UNAUTHORIZED

    user_id = uuid.uuid4()
    refresh_token = create_refresh_token(user_id)
    with pytest.raises(HTTPException) as exc:
        await get_current_user_id(refresh_token)
    assert exc.value.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.asyncio
async def test_get_current_user_id_malformed_uuid():
    token = jwt.encode(
        {"sub": "user_test", "type": "access"},
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
    )
    with pytest.raises(HTTPException) as exc:
        await get_current_user_id(token)
    assert exc.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc.value.detail == "Invalid or expired access token."


@pytest.mark.asyncio
class TestAuthAPI:
    async def test_register_success(self, async_client, db_session):
        email = f"new_{uuid.uuid4().hex[:6]}@example.com"
        resp = await async_client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": "password123"},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["email"] == email

        # Verify user exists in PostgreSQL
        stmt = select(User).where(User.email == email)
        user = (await db_session.execute(stmt)).scalar_one_or_none()
        assert user is not None

    async def test_register_duplicate(self, async_client):
        email = f"dup_{uuid.uuid4().hex[:6]}@example.com"
        resp1 = await async_client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": "password123"},
        )
        assert resp1.status_code == 201

        resp2 = await async_client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": "password123"},
        )
        assert resp2.status_code == 409
        assert "Email already registered" in resp2.json()["detail"]

    async def test_register_concurrent_integrity_error_returns_409(
        self, async_client, monkeypatch
    ):
        """Simulate concurrent TOCTOU race: db.commit() raises IntegrityError due to unique constraint collision."""
        from sqlalchemy.exc import IntegrityError
        from unittest.mock import AsyncMock

        email = f"race_{uuid.uuid4().hex[:6]}@example.com"

        # Mock db.commit() to raise IntegrityError simulating a race collision during registration
        async def mock_commit_failure():
            raise IntegrityError("duplicate key value violates unique constraint", params={}, orig=Exception())

        # Use patch to simulate commit throwing IntegrityError
        from unittest.mock import patch
        with patch("sqlalchemy.ext.asyncio.AsyncSession.commit", side_effect=mock_commit_failure):
            resp = await async_client.post(
                "/api/v1/auth/register",
                json={"email": email, "password": "password123"},
            )
            assert resp.status_code == 409
            assert "Email already registered" in resp.json()["detail"]

    async def test_register_invalid_email_format(self, async_client):
        resp = await async_client.post(
            "/api/v1/auth/register",
            json={"email": "invalid-email-format", "password": "password123"},
        )
        assert resp.status_code == 422

    async def test_register_email_too_long(self, async_client):
        long_email = f"{'a' * 250}@example.com"
        resp = await async_client.post(
            "/api/v1/auth/register",
            json={"email": long_email, "password": "password123"},
        )
        assert resp.status_code == 422

    async def test_register_password_length_bounds(self, async_client):
        valid_email = f"pass_{uuid.uuid4().hex[:6]}@example.com"
        # Password too short (< 6 chars)
        resp_short = await async_client.post(
            "/api/v1/auth/register",
            json={"email": valid_email, "password": "12345"},
        )
        assert resp_short.status_code == 422

        # Password too long (> 72 bytes)
        resp_long = await async_client.post(
            "/api/v1/auth/register",
            json={"email": valid_email, "password": "a" * 73},
        )
        assert resp_long.status_code == 422

    async def test_login_success(self, async_client, redis_client):
        email = f"login_{uuid.uuid4().hex[:6]}@example.com"
        await async_client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": "password123"},
        )

        resp = await async_client.post(
            "/api/v1/auth/login",
            json={"email": email, "password": "password123"},
        )
        assert resp.status_code == 200
        tokens = resp.json()
        assert "access_token" in tokens
        assert "refresh_token" in tokens

        # Verify Refresh Token is physically stored in Redis
        exists = await redis_client.exists(f"refresh_token:{tokens['refresh_token']}")
        assert exists == 1

    async def test_login_failure(self, async_client):
        email = f"fail_{uuid.uuid4().hex[:6]}@example.com"
        await async_client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": "password123"},
        )

        resp = await async_client.post(
            "/api/v1/auth/login",
            json={"email": email, "password": "wrong_password"},
        )
        assert resp.status_code == 401
        assert "Invalid email or password" in resp.json()["detail"]

    async def test_logout_success(self, async_client, redis_client):
        email = f"logout_{uuid.uuid4().hex[:6]}@example.com"
        await async_client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": "password123"},
        )
        login_resp = await async_client.post(
            "/api/v1/auth/login",
            json={"email": email, "password": "password123"},
        )
        refresh_token = login_resp.json()["refresh_token"]

        # Call logout
        resp = await async_client.post(
            "/api/v1/auth/logout",
            json={"refresh_token": refresh_token},
        )
        assert resp.status_code == 200
        assert "Logged out successfully" in resp.json()["message"]

        # Verify token is deleted from Redis
        exists = await redis_client.exists(f"refresh_token:{refresh_token}")
        assert exists == 0

    async def test_logout_invalid_token(self, async_client):
        resp = await async_client.post(
            "/api/v1/auth/logout",
            json={"refresh_token": "nonexistent_token"},
        )
        assert resp.status_code == 400
        assert "Token is already invalid" in resp.json()["detail"]

    async def test_refresh_success(self, async_client, redis_client):
        email = f"refresh_{uuid.uuid4().hex[:6]}@example.com"
        await async_client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": "password123"},
        )
        login_resp = await async_client.post(
            "/api/v1/auth/login",
            json={"email": email, "password": "password123"},
        )
        old_refresh_token = login_resp.json()["refresh_token"]

        resp = await async_client.post(
            "/api/v1/auth/refresh",
            json={"refresh_token": old_refresh_token},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "access_token" in data
        assert "refresh_token" in data
        new_refresh_token = data["refresh_token"]
        assert new_refresh_token != old_refresh_token

        # Verify old token was removed from Redis
        old_exists = await redis_client.exists(f"refresh_token:{old_refresh_token}")
        assert old_exists == 0

        # Verify new rotated token exists in Redis
        new_exists = await redis_client.exists(f"refresh_token:{new_refresh_token}")
        assert new_exists == 1

        # Replay returns 401
        replay_resp = await async_client.post(
            "/api/v1/auth/refresh",
            json={"refresh_token": old_refresh_token},
        )
        assert replay_resp.status_code == 401
        assert "Refresh token has been invalidated or expired" in replay_resp.json()["detail"]

    async def test_refresh_invalidated_by_logout(self, async_client):
        email = f"inv_{uuid.uuid4().hex[:6]}@example.com"
        await async_client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": "password123"},
        )
        login_resp = await async_client.post(
            "/api/v1/auth/login",
            json={"email": email, "password": "password123"},
        )
        refresh_token = login_resp.json()["refresh_token"]

        # Logout first
        await async_client.post(
            "/api/v1/auth/logout",
            json={"refresh_token": refresh_token},
        )

        # Refresh attempt must fail with 401
        resp = await async_client.post(
            "/api/v1/auth/refresh",
            json={"refresh_token": refresh_token},
        )
        assert resp.status_code == 401
        assert "Refresh token has been invalidated or expired" in resp.json()["detail"]

    async def test_refresh_malformed_uuid_sub(self, async_client):
        token = jwt.encode(
            {"sub": "user_test", "type": "refresh"},
            settings.JWT_SECRET_KEY,
            algorithm=settings.JWT_ALGORITHM,
        )
        resp = await async_client.post(
            "/api/v1/auth/refresh",
            json={"refresh_token": token},
        )
        assert resp.status_code == 401
        assert "Invalid or expired refresh token" in resp.json()["detail"]

    async def test_authenticated_route_malformed_uuid_sub_returns_401(self, async_client):
        token = jwt.encode(
            {"sub": "user_test", "type": "access"},
            settings.JWT_SECRET_KEY,
            algorithm=settings.JWT_ALGORITHM,
        )
        resp = await async_client.post(
            "/api/v1/orders/reserve",
            headers={"Authorization": f"Bearer {token}"},
            json={"flight_id": str(uuid.uuid4()), "seat_code": "1A"},
        )
        assert resp.status_code == 401
        assert "Invalid or expired access token" in resp.json()["detail"]

    async def test_register_rate_limiter_blocks_excessive_requests(self, async_client):
        for i in range(settings.RATE_LIMIT_AUTH_TIMES):
            resp = await async_client.post(
                "/api/v1/auth/register",
                json={"email": f"rl_reg_{i}_{uuid.uuid4().hex[:6]}@example.com", "password": "password123"},
            )
            assert resp.status_code == 201

        blocked_resp = await async_client.post(
            "/api/v1/auth/register",
            json={"email": f"rl_reg_blocked_{uuid.uuid4().hex[:6]}@example.com", "password": "password123"},
        )
        assert blocked_resp.status_code == 429
        assert "Too many requests" in blocked_resp.json()["detail"]
        assert int(blocked_resp.headers["Retry-After"]) >= 1
        assert blocked_resp.headers["X-RateLimit-Limit"] == str(settings.RATE_LIMIT_AUTH_TIMES)
        assert blocked_resp.headers["X-RateLimit-Remaining"] == "0"

    async def test_login_rate_limiter_blocks_excessive_requests(self, async_client):
        email = f"rl_login_{uuid.uuid4().hex[:6]}@example.com"
        await async_client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": "password123"},
        )

        for _ in range(settings.RATE_LIMIT_AUTH_TIMES):
            resp = await async_client.post(
                "/api/v1/auth/login",
                json={"email": email, "password": "wrong_password"},
            )
            assert resp.status_code == 401

        blocked_resp = await async_client.post(
            "/api/v1/auth/login",
            json={"email": email, "password": "password123"},
        )
        assert blocked_resp.status_code == 429
        assert "Too many requests" in blocked_resp.json()["detail"]
        assert int(blocked_resp.headers["Retry-After"]) >= 1
        assert blocked_resp.headers["X-RateLimit-Limit"] == str(settings.RATE_LIMIT_AUTH_TIMES)
        assert blocked_resp.headers["X-RateLimit-Remaining"] == "0"

