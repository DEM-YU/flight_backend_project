import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock
import jwt
from fastapi import HTTPException, status
from fastapi.testclient import TestClient

from main import app
from database import get_db, get_redis, settings
from models import User
from auth import (
    hash_password,
    verify_password,
    create_access_token,
    create_refresh_token,
    get_current_user_id,
)

client = TestClient(app)


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

    # decode access token
    payload = jwt.decode(access_token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
    assert payload["sub"] == str(user_id)
    assert payload["type"] == "access"

    # decode refresh token
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
    # Invalid token string
    with pytest.raises(HTTPException) as exc:
        await get_current_user_id("invalid-token")
    assert exc.value.status_code == status.HTTP_401_UNAUTHORIZED

    # Token with wrong type (using refresh token for authorization dependency)
    user_id = uuid.uuid4()
    refresh_token = create_refresh_token(user_id)
    with pytest.raises(HTTPException) as exc:
        await get_current_user_id(refresh_token)
    assert exc.value.status_code == status.HTTP_401_UNAUTHORIZED


class TestAuthAPI:
    def test_register_success(self):
        db_mock = MagicMock()
        db_mock.execute = AsyncMock()
        db_mock.execute.return_value.scalar_one_or_none = MagicMock(return_value=None)
        db_mock.commit = AsyncMock()

        app.dependency_overrides[get_db] = lambda: db_mock
        try:
            resp = client.post(
                "/api/v1/auth/register",
                json={"email": "new@example.com", "password": "password123"}
            )
            assert resp.status_code == 201
            assert resp.json()["email"] == "new@example.com"
            db_mock.commit.assert_called_once()
        finally:
            app.dependency_overrides.clear()

    def test_register_duplicate(self):
        db_mock = MagicMock()
        db_mock.execute = AsyncMock()
        db_mock.execute.return_value.scalar_one_or_none = MagicMock(
            return_value=User(email="exists@example.com")
        )

        app.dependency_overrides[get_db] = lambda: db_mock
        try:
            resp = client.post(
                "/api/v1/auth/register",
                json={"email": "exists@example.com", "password": "password123"}
            )
            assert resp.status_code == 400
            assert "Email already registered" in resp.json()["detail"]
        finally:
            app.dependency_overrides.clear()

    def test_login_success(self):
        db_mock = MagicMock()
        redis_mock = AsyncMock()

        hashed = hash_password("mypassword")
        user = User(id=uuid.uuid4(), email="user@example.com", hashed_password=hashed)

        db_mock.execute = AsyncMock()
        db_mock.execute.return_value.scalar_one_or_none = MagicMock(return_value=user)

        app.dependency_overrides[get_db] = lambda: db_mock
        app.dependency_overrides[get_redis] = lambda: redis_mock
        try:
            resp = client.post(
                "/api/v1/auth/login",
                json={"email": "user@example.com", "password": "mypassword"}
            )
            assert resp.status_code == 200
            data = resp.json()
            assert "access_token" in data
            assert "refresh_token" in data
            redis_mock.setex.assert_called_once()
        finally:
            app.dependency_overrides.clear()

    def test_login_failure(self):
        db_mock = MagicMock()
        db_mock.execute = AsyncMock()
        db_mock.execute.return_value.scalar_one_or_none = MagicMock(return_value=None)

        app.dependency_overrides[get_db] = lambda: db_mock
        try:
            resp = client.post(
                "/api/v1/auth/login",
                json={"email": "user@example.com", "password": "wrong_password"}
            )
            assert resp.status_code == 401
            assert "Invalid email or password" in resp.json()["detail"]
        finally:
            app.dependency_overrides.clear()

    def test_logout_success(self):
        redis_mock = AsyncMock()
        redis_mock.delete.return_value = 1

        app.dependency_overrides[get_redis] = lambda: redis_mock
        try:
            resp = client.post(
                "/api/v1/auth/logout",
                json={"refresh_token": "some_refresh_token"}
            )
            assert resp.status_code == 200
            assert "Logged out successfully" in resp.json()["message"]
            redis_mock.delete.assert_called_once_with("refresh_token:some_refresh_token")
        finally:
            app.dependency_overrides.clear()

    def test_logout_invalid_token(self):
        redis_mock = AsyncMock()
        redis_mock.delete.return_value = 0

        app.dependency_overrides[get_redis] = lambda: redis_mock
        try:
            resp = client.post(
                "/api/v1/auth/logout",
                json={"refresh_token": "nonexistent_token"}
            )
            assert resp.status_code == 400
            assert "Token is already invalid" in resp.json()["detail"]
        finally:
            app.dependency_overrides.clear()

    def test_refresh_success(self):
        redis_mock = AsyncMock()
        redis_mock.exists.return_value = True

        user_id = uuid.uuid4()
        refresh_token = create_refresh_token(user_id)

        app.dependency_overrides[get_redis] = lambda: redis_mock
        try:
            resp = client.post(
                "/api/v1/auth/refresh",
                json={"refresh_token": refresh_token}
            )
            assert resp.status_code == 200
            assert "access_token" in resp.json()
            redis_mock.exists.assert_called_once_with(f"refresh_token:{refresh_token}")
        finally:
            app.dependency_overrides.clear()

    def test_refresh_invalidated_by_logout(self):
        redis_mock = AsyncMock()
        redis_mock.exists.return_value = False

        user_id = uuid.uuid4()
        refresh_token = create_refresh_token(user_id)

        app.dependency_overrides[get_redis] = lambda: redis_mock
        try:
            resp = client.post(
                "/api/v1/auth/refresh",
                json={"refresh_token": refresh_token}
            )
            assert resp.status_code == 401
            assert "Refresh token has been invalidated or expired" in resp.json()["detail"]
        finally:
            app.dependency_overrides.clear()
