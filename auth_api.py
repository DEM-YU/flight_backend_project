from typing import Annotated
import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import jwt

from database import get_db, get_redis, settings
from models import (
    User,
    RegisterRequest,
    LoginRequest,
    TokenResponse,
    LogoutRequest,
    RefreshRequest,
)
from auth import (
    hash_password,
    verify_password,
    create_access_token,
    create_refresh_token,
)

router = APIRouter(prefix="/api/v1/auth", tags=["Auth"])


@router.post("/register", status_code=status.HTTP_201_CREATED)
async def register(payload: RegisterRequest, db: Annotated[AsyncSession, Depends(get_db)]):
    """Register a new user with bcrypt-hashed password."""
    stmt = select(User).where(User.email == payload.email.strip().lower())
    result = await db.execute(stmt)
    if result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered."
        )

    hashed = hash_password(payload.password)
    user = User(email=payload.email.strip().lower(), hashed_password=hashed)
    db.add(user)
    await db.commit()
    return {"message": "User registered successfully.", "email": user.email}


@router.post("/login", response_model=TokenResponse)
async def login(
    payload: LoginRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
):
    """Authenticate credentials and issue short-lived Access + long-lived Redis Refresh token."""
    stmt = select(User).where(User.email == payload.email.strip().lower())
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()

    if not user or not verify_password(payload.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password."
        )

    access_token = create_access_token(user.id)
    refresh_token = create_refresh_token(user.id)

    # Store refresh token in Redis with key 'refresh_token:<token>'
    redis_key = f"refresh_token:{refresh_token}"
    ttl_seconds = settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 3600
    await redis.setex(redis_key, ttl_seconds, str(user.id))

    return TokenResponse(access_token=access_token, refresh_token=refresh_token)


@router.post("/logout")
async def logout(
    payload: LogoutRequest,
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
):
    """Invalidate a session by deleting the Refresh Token from Redis."""
    redis_key = f"refresh_token:{payload.refresh_token}"
    deleted = await redis.delete(redis_key)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Token is already invalid or does not exist."
        )
    return {"message": "Logged out successfully. Token invalidated."}


@router.post("/refresh")
async def refresh(
    payload: RefreshRequest,
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
):
    """Issue a new Access Token if the provided Refresh Token is valid and exists in Redis."""
    try:
        decoded = jwt.decode(
            payload.refresh_token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM]
        )
        token_type = decoded.get("type")
        user_id_str = decoded.get("sub")
        if not user_id_str or token_type != "refresh":
            raise jwt.PyJWTError()
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token."
        )

    # Check Redis to ensure it wasn't manually invalidated
    redis_key = f"refresh_token:{payload.refresh_token}"
    exists = await redis.exists(redis_key)
    if not exists:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token has been invalidated or expired."
        )

    new_access_token = create_access_token(user_id_str)
    return {"access_token": new_access_token, "token_type": "bearer"}
