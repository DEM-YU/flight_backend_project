from typing import Annotated
import uuid
import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
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
from rate_limiter import RateLimiter

router = APIRouter(prefix="/api/v1/auth", tags=["Auth"])

auth_limiter = RateLimiter(
    times=settings.RATE_LIMIT_AUTH_TIMES,
    seconds=settings.RATE_LIMIT_AUTH_SECONDS,
)


@router.post(
    "/register",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(auth_limiter)],
)
async def register(req: RegisterRequest, db: Annotated[AsyncSession, Depends(get_db)]):
    email = req.email.strip().lower()
    stmt = select(User).where(User.email == email)
    existing_user = (await db.execute(stmt)).scalar_one_or_none()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email already registered."
        )

    hashed = hash_password(req.password)
    user = User(email=email, hashed_password=hashed)
    db.add(user)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email already registered."
        )
    return {"message": "User registered successfully.", "email": user.email}


@router.post(
    "/login",
    response_model=TokenResponse,
    dependencies=[Depends(auth_limiter)],
)
async def login(
    req: LoginRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
):
    stmt = select(User).where(User.email == req.email.strip().lower())
    user = (await db.execute(stmt)).scalar_one_or_none()

    if not user or not verify_password(req.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password."
        )

    access_token = create_access_token(user.id)
    refresh_token = create_refresh_token(user.id)

    redis_key = f"refresh_token:{refresh_token}"
    ttl_seconds = settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 3600
    await redis.setex(redis_key, ttl_seconds, str(user.id))

    return TokenResponse(access_token=access_token, refresh_token=refresh_token)


@router.post("/logout")
async def logout(
    req: LogoutRequest,
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
):
    redis_key = f"refresh_token:{req.refresh_token}"
    deleted = await redis.delete(redis_key)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Token is already invalid or does not exist."
        )
    return {"message": "Logged out successfully. Token invalidated."}


@router.post("/refresh", response_model=TokenResponse)
async def refresh(
    req: RefreshRequest,
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
):
    try:
        claims = jwt.decode(
            req.refresh_token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM]
        )
        token_type = claims.get("type")
        user_id_str = claims.get("sub")
        if not user_id_str or token_type != "refresh":
            raise jwt.PyJWTError()
        user_id = uuid.UUID(str(user_id_str))
    except (jwt.PyJWTError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token."
        )

    # Rotate refresh token
    redis_key = f"refresh_token:{req.refresh_token}"
    deleted = await redis.delete(redis_key)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token has been invalidated or expired."
        )

    new_access_token = create_access_token(user_id)
    new_refresh_token = create_refresh_token(user_id)

    new_redis_key = f"refresh_token:{new_refresh_token}"
    ttl_seconds = settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 3600
    await redis.setex(new_redis_key, ttl_seconds, str(user_id))

    return TokenResponse(
        access_token=new_access_token,
        refresh_token=new_refresh_token,
        token_type="bearer",
    )
