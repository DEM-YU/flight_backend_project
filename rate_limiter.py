import time
import uuid
from typing import Annotated

import jwt
import redis.asyncio as aioredis
from fastapi import Depends, HTTPException, Request, status

from database import get_redis, settings

SLIDING_WINDOW_SCRIPT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local current_count = redis.call('ZCARD', key)

if current_count < limit then
    redis.call('ZADD', key, now, member)
    redis.call('EXPIRE', key, math.ceil(window))
    return {1, limit - (current_count + 1), 0}
else
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local retry_after = 1
    if #oldest >= 2 then
        retry_after = math.max(1, math.ceil((tonumber(oldest[2]) + window) - now))
    end
    return {0, 0, retry_after}
end
"""


class RateLimiter:
    def __init__(self, times: int = 5, seconds: int = 10, key_prefix: str = "rate_limit"):
        self.times = times
        self.seconds = seconds
        self.key_prefix = key_prefix

    def _extract_identifier(self, request: Request) -> str:
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ", 1)[1]
            try:
                claims = jwt.decode(
                    token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM]
                )
                if claims.get("type") == "access" and claims.get("sub"):
                    return f"user:{claims['sub']}"
            except jwt.PyJWTError:
                pass

        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            client_host = forwarded.split(",")[0].strip()
        else:
            client_host = request.client.host if request.client else "unknown"
        return f"ip:{client_host}"

    async def __call__(
        self,
        request: Request,
        redis: Annotated[aioredis.Redis, Depends(get_redis)],
    ) -> None:
        identifier = self._extract_identifier(request)
        path = request.url.path
        redis_key = f"{self.key_prefix}:{path}:{identifier}"

        now = time.time()
        member = f"{now}:{uuid.uuid4()}"

        limiter_res = await redis.eval(
            SLIDING_WINDOW_SCRIPT,
            1,
            redis_key,
            now,
            self.seconds,
            self.times,
            member,
        )

        allowed = bool(limiter_res[0])
        retry_after = int(limiter_res[2])

        if not allowed:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many requests. Please try again later.",
                headers={
                    "Retry-After": str(retry_after),
                    "X-RateLimit-Limit": str(self.times),
                    "X-RateLimit-Remaining": "0",
                },
            )
