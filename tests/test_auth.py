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
        assert resp2.status_code == 400
        assert "Email already registered" in resp2.json()["detail"]

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

    async def test_refresh_success(self, async_client):
        email = f"refresh_{uuid.uuid4().hex[:6]}@example.com"
        await async_client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": "password123"},
        )
        login_resp = await async_client.post(
            "/api/v1/auth/login",
            json={"email": email, "password": "password123"},
        )
        refresh_token = login_resp.json()["refresh_token"]

        resp = await async_client.post(
            "/api/v1/auth/refresh",
            json={"refresh_token": refresh_token},
        )
        assert resp.status_code == 200
        assert "access_token" in resp.json()

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
