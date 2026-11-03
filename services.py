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
from models import FlightResponse, OrderStatus, SeatState, Flight, Order, Seat

logger = logging.getLogger(__name__)

_CACHE_TTL_BASE = 1800
_CACHE_TTL_JITTER = 300
# Short TTL for empty results so newly added flights become visible quickly.
_CACHE_EMPTY_TTL = 60

_ORDER_TIMEOUT_SECONDS = 60
_SEAT_RELEASE_MAX_RETRIES = 3

ORDER_TIMEOUT_QUEUE_KEY = "order:timeout:zset"

# Atomic pop script for Redis ZSet: fetches expired items and removes them in one step
ZPOPMIN_BY_SCORE_SCRIPT = """
local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #items > 0 then
    redis.call('ZREM', KEYS[1], unpack(items))
end
return items
"""




class SeatUnavailableException(Exception):
    """Seat already locked, reservation failed."""


class SeatNotFoundException(Exception):
    """Seat does not exist or Redis key is not initialized."""


class OrderNotFoundException(Exception):
    """Order does not exist."""


class OrderForbiddenException(Exception):
    """User does not have permission to access this order."""


class InvalidOrderStatusException(Exception):
    """Order is not in a payable state."""


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


async def schedule_order_timeout(
    redis: aioredis.Redis,
    order_id: str,
    flight_id: str,
    seat_code: str,
    delay: int = _ORDER_TIMEOUT_SECONDS,
) -> None:
    """Register an order in the Redis Sorted Set (ZSet) delay queue.
    
    The score is the UNIX timestamp when the payment window expires.
    Because tasks are persisted in Redis, they survive server crashes and restarts.
    """
    expire_at = datetime.now(timezone.utc).timestamp() + delay
    payload = json.dumps({
        "order_id": order_id,
        "flight_id": flight_id,
        "seat_code": seat_code,
    })
    await redis.zadd(ORDER_TIMEOUT_QUEUE_KEY, {payload: expire_at})
    logger.info("Order %s scheduled for timeout in %ds via Redis ZSet", order_id, delay)


async def cancel_expired_order(
    redis: aioredis.Redis,
    order_id: str,
    flight_id: str,
    seat_code: str,
) -> bool:
    """Check order status in DB; if still PENDING, cancel it and release the Redis seat lock.
    
    Returns True if an order was cancelled or already confirmed, False on DB miss.
    """
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
            return False

        # Cancel the pending order
        order.status = OrderStatus.CANCELLED
        await db.commit()
        logger.info("Order %s auto-cancelled by timeout worker", order_id)

    # Release seat lock with linear backoff retries to survive network hiccups
    seat_key = f"flight:{flight_id}:seats"
    for attempt in range(_SEAT_RELEASE_MAX_RETRIES):
        try:
            await redis.hset(seat_key, seat_code, SeatState.AVAILABLE)
            logger.info(
                "Seat %s released on flight %s after order %s cancellation",
                seat_code, flight_id, order_id,
            )
            return True
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
    return True


async def process_expired_orders(
    redis: aioredis.Redis,
    batch_size: int = 50,
) -> int:
    """Fetch and cancel expired orders from the Redis ZSet.
    
    Uses an atomic Lua script (ZRANGEBYSCORE + ZREM) to prevent race conditions
    when multiple worker instances or app replicas poll concurrently.
    """
    now_ts = datetime.now(timezone.utc).timestamp()
    raw_items = await redis.eval(
        ZPOPMIN_BY_SCORE_SCRIPT, 1, ORDER_TIMEOUT_QUEUE_KEY, now_ts, batch_size
    )
    if not raw_items:
        return 0

    processed = 0
    for item_str in raw_items:
        try:
            data = json.loads(item_str)
            await cancel_expired_order(
                redis, data["order_id"], data["flight_id"], data["seat_code"]
            )
            processed += 1
        except Exception:
            logger.error("Error processing expired order payload %s", item_str, exc_info=True)

    return processed


async def run_timeout_worker(
    redis: aioredis.Redis,
    poll_interval: float = 1.0,
) -> None:
    """Continuously poll the Redis ZSet for expired orders until cancelled."""
    logger.info("Started Redis ZSet timeout cancellation worker.")
    try:
        while True:
            try:
                await process_expired_orders(redis)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.error("Error in timeout worker poll cycle", exc_info=True)
            await asyncio.sleep(poll_interval)
    finally:
        logger.info("Stopped Redis ZSet timeout cancellation worker.")


async def process_order_timeout(
    redis: aioredis.Redis,
    order_id: str,
    flight_id: str,
    seat_code: str,
    delay: int = _ORDER_TIMEOUT_SECONDS,
) -> None:
    """Legacy timeout handler kept for backwards compatibility with tests."""
    await asyncio.sleep(delay)
    await cancel_expired_order(redis, order_id, flight_id, seat_code)


async def pay_order(
    db: AsyncSession,
    order_id: UUID,
    user_id: UUID,
) -> Order:
    """Confirm payment for an order and permanently mark the seat as occupied in PostgreSQL."""
    result = await db.execute(select(Order).where(Order.id == order_id))
    order = result.scalar_one_or_none()

    if order is None:
        raise OrderNotFoundException(f"Order {order_id} not found.")

    if order.user_id != user_id:
        raise OrderForbiddenException("You do not have permission to pay for this order.")

    if order.status != OrderStatus.PENDING:
        raise InvalidOrderStatusException(
            f"Order {order_id} cannot be paid. Current status: {order.status}."
        )

    order.status = OrderStatus.CONFIRMED

    # Mark seat status as occupied (1) in persistent PG storage
    seat_stmt = select(Seat).where(
        Seat.flight_id == order.flight_id,
        Seat.seat_code == order.seat_code,
    )
    seat_result = await db.execute(seat_stmt)
    seat = seat_result.scalar_one_or_none()
    if seat:
        seat.status = 1

    await db.commit()
    await db.refresh(order)
    logger.info("Order %s confirmed and paid by user %s", order_id, user_id)
    return order
