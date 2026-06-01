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

# Cache Configuration

_CACHE_TTL_BASE = 1800       # 30-minute base duration
_CACHE_TTL_JITTER = 300      # ±5 minutes randomization to mitigate stampedes
_CACHE_EMPTY_TTL = 60        # Short TTL for empty results to prevent cache penetration

# Order Timeout Configuration

_ORDER_TIMEOUT_SECONDS = 60  # Payment window before auto-cancellation
_SEAT_RELEASE_MAX_RETRIES = 3

# Custom Exceptions


class SeatUnavailableException(Exception):
    """Seat already locked, reservation failed."""


class SeatNotFoundException(Exception):
    """Seat does not exist or Redis key is not initialized."""


# ── Lua Script: Atomic seat checking and locking mechanism ──
# KEYS[1] : Hash table name, structured as flight:{flight_id}:seats
# ARGV[1] : Target seat identifier, e.g., '12A'
# Returns  1 = Lock acquired successfully
# Returns  0 = Already locked by another reservation
# Returns -1 = Seat does not exist (HGET returns nil)
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
    """Build a normalized cache key (uppercase city codes to prevent duplicates)."""
    return f"route:{departure.upper()}:{arrival.upper()}:{date_str}"


async def search_flights(
    db: AsyncSession,
    redis: aioredis.Redis,
    departure: str,
    arrival: str,
    travel_date: date,
) -> tuple[list[FlightResponse], bool]:
    """
    Query flight data implementing the Cache-Aside pattern.

    Returns:
        tuple: (flights, is_from_cache) to provide granular layer observability.
    """
    date_str = travel_date.isoformat()
    key = _cache_key(departure, arrival, date_str)

    # Step A: Cache Hit
    cached = await redis.get(key)
    if cached is not None:
        data = json.loads(cached)
        return [FlightResponse(**item) for item in data], True

    # Step B: Cache Miss → Query DB
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

    # Step C: Write-Back with jitter TTL; cache empty results with short TTL
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
    """
    Atomic seat reservation: Redis Lua concurrency guard -> PostgreSQL order persistence.

    Raises:
        SeatNotFoundException:    Seat key does not exist in Redis.
        SeatUnavailableException: Seat is already locked by another reservation.
    """
    seat_key = f"flight:{flight_id}:seats"

    # Step 1: Redis atomic concurrency control to prevent overselling
    result = await redis.eval(RESERVE_SEAT_SCRIPT, 1, seat_key, seat_code)
    if result == -1:
        raise SeatNotFoundException(
            f"Seat {seat_code} does not exist on flight {flight_id}."
        )
    if result == 0:
        raise SeatUnavailableException(
            f"Seat {seat_code} on flight {flight_id} is unavailable."
        )

    # Step 2: PostgreSQL transactional order persistence with compensating rollback
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
        # Compensating transaction: release the Redis lock to prevent ghost locks
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
    """
    Asynchronous order timeout and automatic release worker (BackgroundTask).

    Execution Workflow:
      1. sleep(delay) enforces the business payment window countdown asynchronously.
      2. Utilizes an isolated Session for DB queries to decouple from closed HTTP contexts.
      3. If status remains 'Pending' -> Transition to 'Cancelled' and commit transaction.
      4. Redis HSET resets seat state to '0', returning inventory back to the ticket pool.
    """
    # Step A: Non-blocking delay for the business payment window
    await asyncio.sleep(delay)

    # Step B: Isolated database session (decoupled from HTTP request lifecycle)
    async with AsyncSessionLocal() as db:
        # Step C: State machine transition
        result = await db.execute(
            select(Order).where(Order.id == UUID(order_id))
        )
        order = result.scalar_one_or_none()

        if order is None or order.status != OrderStatus.PENDING:
            # Order already paid or processed by alternative workflows, idempotent exit
            logger.info(
                "Order %s timeout check: status=%s, no action needed",
                order_id, order.status if order else "NOT_FOUND",
            )
            return

        order.status = OrderStatus.CANCELLED
        await db.commit()
        logger.info("Order %s auto-cancelled after %ds timeout", order_id, delay)

    # Step D: Cache rollback with retry and logging
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

    # All retries exhausted — critical alert
    logger.critical(
        "GHOST LOCK: Seat %s on flight %s is locked in Redis but order %s "
        "is cancelled in DB. Manual intervention required.",
        seat_code, flight_id, order_id,
    )