import asyncio
import json
import logging
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from uuid import UUID

import redis.asyncio as aioredis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal
from models import FlightResponse, OrderStatus, SeatState, Flight, Order

logger = logging.getLogger(__name__)

_CACHE_TTL_BASE = 1800
_CACHE_TTL_JITTER = 300
# Short TTL for empty results so newly added flights become visible quickly.
_CACHE_EMPTY_TTL = 60

_ORDER_TIMEOUT_SECONDS = 60
_SEAT_RELEASE_MAX_RETRIES = 3




class SeatUnavailableException(Exception):
    """Seat already locked, reservation failed."""


class SeatNotFoundException(Exception):
    """Seat does not exist or Redis key is not initialized."""


# Lua check-and-set: HGET nil means seat was never initialized, distinct from
# already-locked. Single EVAL round-trip avoids WATCH/MULTI race windows.
# TODO: register the script with SCRIPT LOAD and call via EVALSHA to save
# bandwidth on repeated invocations under high concurrency.
RESERVE_SEAT_SCRIPT = """
local status = redis.call('HGET', KEYS[1], ARGV[1])
if status == false then
    return -1
elseif status == '0' then
    redis.call('HSET', KEYS[1], ARGV[1], '1')
    return 1
else
    return 0
end
"""


def _cache_key(departure: str, arrival: str, date_str: str) -> str:
    return f"route:{departure.upper()}:{arrival.upper()}:{date_str}"


async def search_flights(
    db: AsyncSession,
    redis: aioredis.Redis,
    departure: str,
    arrival: str,
    travel_date: date,
) -> tuple[list[FlightResponse], bool]:
    """Cache-Aside flight query by route and date. Returns (results, cache_hit)."""
    date_str = travel_date.isoformat()
    key = _cache_key(departure, arrival, date_str)

    # 1. Try cache
    cached = await redis.get(key)
    if cached is not None:
        data = json.loads(cached)
        return [FlightResponse(**item) for item in data], True

    # 2. Cache miss, query PG
    day_start = datetime(
        travel_date.year, travel_date.month, travel_date.day,
        tzinfo=timezone.utc,
    )
    day_end = day_start + timedelta(days=1)

    stmt = select(Flight).where(
        Flight.departure == departure,
        Flight.arrival == arrival,
        Flight.departure_time >= day_start,
        Flight.departure_time < day_end,
    )

    result = await db.execute(stmt)
    flights = result.scalars().all()
    schemas = [FlightResponse.model_validate(f) for f in flights]

    # 3. Write back with jittered TTL
    # Jitter avoids thundering-herd on TTL expiry across multiple keys.
    # TODO: add a bloom filter layer in front to reject impossible routes
    # without hitting Redis or PG at all.
    payload = json.dumps([s.model_dump(mode="json") for s in schemas])
    ttl = (
        _CACHE_EMPTY_TTL if not schemas
        else _CACHE_TTL_BASE + random.randint(-_CACHE_TTL_JITTER, _CACHE_TTL_JITTER)
    )
    await redis.setex(key, ttl, payload)

    return schemas, False


async def reserve_seat(
    db: AsyncSession,
    redis: aioredis.Redis,
    user_id: UUID,
    flight_id: UUID,
    seat_code: str,
) -> Order:
    """Acquire a Redis seat lock via Lua, then persist a Pending order to PG."""
    seat_key = f"flight:{flight_id}:seats"

    # 1. Atomic seat lock via Lua
    result = await redis.eval(RESERVE_SEAT_SCRIPT, 1, seat_key, seat_code)
    if result == -1:
        raise SeatNotFoundException(
            f"Seat {seat_code} does not exist on flight {flight_id}."
        )
    if result == 0:
        raise SeatUnavailableException(
            f"Seat {seat_code} on flight {flight_id} is unavailable."
        )

    # 2. Persist order, with compensating Redis rollback on failure.
    # If PG write fails after Redis lock, we must roll back the lock.
    # Without this, the seat stays locked forever with no matching order.
    try:
        order = Order(
            id=uuid.uuid4(),
            user_id=user_id,
            flight_id=flight_id,
            seat_code=seat_code,
            status=OrderStatus.PENDING,
        )
        db.add(order)
        await db.commit()
        await db.refresh(order)
        logger.info(
            "Seat %s locked on flight %s for user %s, order %s",
            seat_code, flight_id, user_id, order.id,
        )
        return order
    except Exception:
        logger.error(
            "DB commit failed for seat %s on flight %s, rolling back Redis lock",
            seat_code, flight_id, exc_info=True,
        )
        await db.rollback()
        await redis.hset(seat_key, seat_code, SeatState.AVAILABLE)
        raise


async def process_order_timeout(
    redis: aioredis.Redis,
    order_id: str,
    flight_id: str,
    seat_code: str,
    delay: int = _ORDER_TIMEOUT_SECONDS,
) -> None:
    """Background task: auto-cancel unpaid orders after the payment window expires."""
    # TODO: replace with a durable job queue (Celery / pg-based outbox) so
    # timeouts survive process restarts. In-process sleep is a pragmatic
    # starting point but loses tasks on crash.

    # 1. Wait for payment window
    await asyncio.sleep(delay)

    # 2. Check order status in an isolated session
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Order).where(Order.id == UUID(order_id))
        )
        order = result.scalar_one_or_none()

        if order is None or order.status != OrderStatus.PENDING:
            logger.info(
                "Order %s timeout check: status=%s, no action needed",
                order_id, order.status if order else "NOT_FOUND",
            )
            return

        # 3. Cancel the order
        order.status = OrderStatus.CANCELLED
        await db.commit()
        logger.info("Order %s auto-cancelled after %ds timeout", order_id, delay)

    # 4. Release seat lock with retries
    # Retry with linear backoff; Redis blip shouldn't leave a ghost lock.
    seat_key = f"flight:{flight_id}:seats"
    for attempt in range(_SEAT_RELEASE_MAX_RETRIES):
        try:
            await redis.hset(seat_key, seat_code, SeatState.AVAILABLE)
            logger.info(
                "Seat %s released on flight %s after order %s cancellation",
                seat_code, flight_id, order_id,
            )
            return
        except Exception:
            logger.error(
                "Failed to release seat %s on flight %s (attempt %d/%d)",
                seat_code, flight_id, attempt + 1, _SEAT_RELEASE_MAX_RETRIES,
                exc_info=True,
            )
            if attempt < _SEAT_RELEASE_MAX_RETRIES - 1:
                await asyncio.sleep(1 * (attempt + 1))

    # Retries exhausted. Seat is locked in Redis but cancelled in PG.
    logger.critical(
        "GHOST LOCK: Seat %s on flight %s is locked in Redis but order %s "
        "is cancelled in DB. Manual intervention required.",
        seat_code, flight_id, order_id,
    )