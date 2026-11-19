import asyncio
import json
import logging
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from uuid import UUID

import redis.asyncio as aioredis
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal
from models import (
    Flight,
    FlightResponse,
    FlightSeatMapResponse,
    Order,
    OrderStatus,
    Seat,
    SeatResponse,
    SeatState,
)

logger = logging.getLogger(__name__)

_CACHE_TTL_BASE = 1800
_CACHE_TTL_JITTER = 300
# Negative cache TTL
_CACHE_EMPTY_TTL = 60

_ORDER_TIMEOUT_SECONDS = 60
_ORDER_TIMEOUT_RETRY_DELAY = 5
_ORDER_TIMEOUT_MAX_RETRIES = 3
_SEAT_RELEASE_MAX_RETRIES = 3

ORDER_TIMEOUT_QUEUE_KEY = "order:timeout:zset"

ZPOPMIN_BY_SCORE_SCRIPT = """
local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #items > 0 then
    redis.call('ZREM', KEYS[1], unpack(items))
end
return items
"""

HYDRATE_SEAT_MAP_SCRIPT = """
local key = KEYS[1]
for i = 1, #ARGV, 2 do
    redis.call('HSETNX', key, ARGV[i], ARGV[i + 1])
end
return 1
"""

HSET_IF_KEY_EXISTS_SCRIPT = """
if redis.call('EXISTS', KEYS[1]) == 1 then
    redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
    return 1
else
    return 0
end
"""




class BookingDomainException(Exception):
    status_code: int = 500
    detail: str = "Internal server error"

    def __init__(self, detail: str | None = None, status_code: int | None = None):
        if detail:
            self.detail = detail
        if status_code:
            self.status_code = status_code
        super().__init__(self.detail)


class FlightNotFoundException(BookingDomainException):
    status_code = 404

    def __init__(self, flight_id: str | UUID):
        super().__init__(f"Flight {flight_id} not found.")


class SeatNotFoundException(BookingDomainException):
    status_code = 404

    def __init__(self, detail: str = "Seat does not exist or flight seat map is not initialized."):
        super().__init__(detail)


class SeatUnavailableException(BookingDomainException):
    status_code = 409

    def __init__(self, detail: str = "Seat is already reserved or unavailable."):
        super().__init__(detail)


class OrderNotFoundException(BookingDomainException):
    status_code = 404

    def __init__(self, order_id: str | UUID):
        super().__init__(f"Order {order_id} not found.")


class OrderForbiddenException(BookingDomainException):
    status_code = 403

    def __init__(self, detail: str = "You do not have permission to access this order."):
        super().__init__(detail)


class InvalidOrderStatusException(BookingDomainException):
    status_code = 409

    def __init__(self, detail: str = "Order already processed or expired."):
        super().__init__(detail)


# Returns: -1 (uninitialized), 1 (locked), 0 (unavailable)
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


async def _hset_seat_if_exists(
    redis: aioredis.Redis,
    flight_id: str | UUID,
    seat_code: str,
    state: str | int,
) -> bool:
    seat_key = f"flight:{flight_id}:seats"
    res = await redis.eval(HSET_IF_KEY_EXISTS_SCRIPT, 1, seat_key, seat_code, str(state))
    return res == 1


async def search_flights(
    db: AsyncSession,
    redis: aioredis.Redis,
    departure: str,
    arrival: str,
    travel_date: date,
) -> tuple[list[FlightResponse], bool]:
    date_str = travel_date.isoformat()
    key = _cache_key(departure, arrival, date_str)

    cached = await redis.get(key)
    if cached is not None:
        cached_flights = json.loads(cached)
        return [FlightResponse(**f) for f in cached_flights], True

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

    flights = (await db.execute(stmt)).scalars().all()
    flight_responses = [FlightResponse.model_validate(f) for f in flights]

    # Base TTL + jitter
    cache_payload = json.dumps([f.model_dump(mode="json") for f in flight_responses])
    ttl = (
        _CACHE_EMPTY_TTL if not flight_responses
        else _CACHE_TTL_BASE + random.randint(-_CACHE_TTL_JITTER, _CACHE_TTL_JITTER)
    )
    await redis.setex(key, ttl, cache_payload)

    return flight_responses, False


def _seat_sort_key(code: str) -> tuple[int, str]:
    digits = "".join(ch for ch in code if ch.isdigit())
    num = int(digits) if digits else 0
    return (num, code)


async def get_flight_seat_map(
    db: AsyncSession,
    redis: aioredis.Redis,
    flight_id: UUID,
) -> FlightSeatMapResponse:
    seat_key = f"flight:{flight_id}:seats"
    seat_map = await redis.hgetall(seat_key)

    if not seat_map:
        # Query DB on cache miss
        flight = (
            await db.execute(select(Flight).where(Flight.id == flight_id))
        ).scalar_one_or_none()
        if flight is None:
            raise FlightNotFoundException(f"Flight {flight_id} not found.")

        seats = (
            await db.execute(
                select(Seat).where(Seat.flight_id == flight_id)
            )
        ).scalars().all()

        if seats:
            # Sync active orders so unconfirmed reservations display as locked
            active_orders_stmt = select(Order.seat_code).where(
                Order.flight_id == flight_id,
                Order.status.in_([OrderStatus.PENDING, OrderStatus.CONFIRMED]),
            )
            active_seats = set(
                (await db.execute(active_orders_stmt)).scalars().all()
            )

            args = []
            for s in seats:
                is_locked = (s.status == 1 or s.seat_code in active_seats)
                args.extend([s.seat_code, str(SeatState.LOCKED if is_locked else SeatState.AVAILABLE)])

            # HSETNX prevents overwriting concurrent locks
            await redis.eval(HYDRATE_SEAT_MAP_SCRIPT, 1, seat_key, *args)
            seat_map = await redis.hgetall(seat_key)
        else:
            seat_map = {}

    seat_list = []
    available_count = 0
    for code in sorted(seat_map.keys(), key=_seat_sort_key):
        status_val = int(seat_map[code])
        is_avail = (status_val == 0)
        if is_avail:
            available_count += 1
        seat_list.append(
            SeatResponse(
                seat_code=code,
                status=status_val,
                is_available=is_avail,
            )
        )

    return FlightSeatMapResponse(
        flight_id=flight_id,
        total_seats=len(seat_list),
        available_seats=available_count,
        seats=seat_list,
    )


async def schedule_order_timeout(
    redis: aioredis.Redis,
    order_id: str,
    flight_id: str,
    seat_code: str,
    delay: int = _ORDER_TIMEOUT_SECONDS,
) -> None:
    expire_at = datetime.now(timezone.utc).timestamp() + delay
    order_task = json.dumps({
        "order_id": order_id,
        "flight_id": flight_id,
        "seat_code": seat_code,
    })
    await redis.zadd(ORDER_TIMEOUT_QUEUE_KEY, {order_task: expire_at})
    logger.info("Order %s scheduled for timeout in %ds via Redis ZSet", order_id, delay)


async def reserve_seat(
    db: AsyncSession,
    redis: aioredis.Redis,
    user_id: UUID,
    flight_id: UUID,
    seat_code: str,
    timeout_delay: int = _ORDER_TIMEOUT_SECONDS,
) -> Order:
    seat_key = f"flight:{flight_id}:seats"

    lock_status = await redis.eval(RESERVE_SEAT_SCRIPT, 1, seat_key, seat_code)
    if lock_status == -1:
        # Hydrate from DB on miss and retry lock once
        try:
            await get_flight_seat_map(db, redis, flight_id)
            lock_status = await redis.eval(RESERVE_SEAT_SCRIPT, 1, seat_key, seat_code)
        except FlightNotFoundException:
            pass

    if lock_status == -1:
        raise SeatNotFoundException(
            f"Seat {seat_code} does not exist on flight {flight_id}."
        )
    if lock_status == 0:
        raise SeatUnavailableException(
            f"Seat {seat_code} on flight {flight_id} is unavailable."
        )

    # 1. Persist pending order
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
    except IntegrityError as exc:
        await db.rollback()
        err_msg = str(exc).lower()
        if "uq_flight_seat_active_order" in err_msg:
            logger.warning(
                "Database rejected reservation for seat %s on flight %s due to duplicate active order: %s",
                seat_code, flight_id, exc,
            )
            # Active order exists in DB; sync Redis to locked if key exists
            await _hset_seat_if_exists(redis, flight_id, seat_code, SeatState.LOCKED)
            raise SeatUnavailableException(
                f"Seat {seat_code} on flight {flight_id} is unavailable."
            ) from exc
        else:
            # Rollback Redis lock on other integrity error if key exists
            logger.error(
                "DB integrity violation for seat %s on flight %s, rolling back Redis lock: %s",
                seat_code, flight_id, exc,
            )
            await _hset_seat_if_exists(redis, flight_id, seat_code, SeatState.AVAILABLE)
            raise
    except Exception:
        logger.error(
            "DB commit failed for seat %s on flight %s, rolling back Redis lock",
            seat_code, flight_id, exc_info=True,
        )
        await db.rollback()
        await _hset_seat_if_exists(redis, flight_id, seat_code, SeatState.AVAILABLE)
        raise

    # 2. Enqueue timeout task; compensate on failure
    try:
        await schedule_order_timeout(
            redis, str(order.id), str(flight_id), seat_code, delay=timeout_delay
        )
    except Exception as queue_exc:
        logger.error(
            "Failed to schedule timeout queue for order %s on flight %s, executing compensating rollback",
            order.id, flight_id, exc_info=True,
        )
        try:
            stmt = (
                update(Order)
                .where(Order.id == order.id, Order.status == OrderStatus.PENDING)
                .values(status=OrderStatus.CANCELLED)
            )
            await db.execute(stmt)
            await db.commit()
        except Exception:
            await db.rollback()
            logger.error(
                "Failed to cancel DB order %s during timeout queue compensation",
                order.id,
                exc_info=True,
            )
        await release_seat_with_retry(redis, flight_id, seat_code, order.id)
        raise RuntimeError(
            f"Failed to schedule expiration for order {order.id}. Reservation cancelled."
        ) from queue_exc

    logger.info(
        "Seat %s locked on flight %s for user %s, order %s scheduled for timeout in %ds",
        seat_code, flight_id, user_id, order.id, timeout_delay,
    )
    return order


async def cancel_expired_order(
    redis: aioredis.Redis,
    order_id: str,
    flight_id: str,
    seat_code: str,
    db: AsyncSession | None = None,
) -> bool:
    if db is not None:
        # Cancel only if order is still PENDING
        stmt = (
            update(Order)
            .where(Order.id == UUID(order_id), Order.status == OrderStatus.PENDING)
            .values(status=OrderStatus.CANCELLED)
        )
        result = await db.execute(stmt)
        await db.commit()

        if result.rowcount == 0:
            logger.info(
                "Order %s timeout check: order not found or already processed (paid/cancelled), skipping seat release",
                order_id,
            )
            return False

        logger.info("Order %s auto-cancelled by timeout worker", order_id)
        return await release_seat_with_retry(redis, flight_id, seat_code, order_id)

    async with AsyncSessionLocal() as session:
        # Cancel only if order is still PENDING
        stmt = (
            update(Order)
            .where(Order.id == UUID(order_id), Order.status == OrderStatus.PENDING)
            .values(status=OrderStatus.CANCELLED)
        )
        result = await session.execute(stmt)
        await session.commit()

        if result.rowcount == 0:
            logger.info(
                "Order %s timeout check: order not found or already processed (paid/cancelled), skipping seat release",
                order_id,
            )
            return False

        logger.info("Order %s auto-cancelled by timeout worker", order_id)

    return await release_seat_with_retry(redis, flight_id, seat_code, order_id)


async def release_seat_with_retry(
    redis: aioredis.Redis,
    flight_id: str | UUID,
    seat_code: str,
    order_id: str | UUID,
    max_retries: int = _SEAT_RELEASE_MAX_RETRIES,
) -> bool:
    for attempt in range(max_retries):
        try:
            updated = await _hset_seat_if_exists(
                redis, flight_id, seat_code, SeatState.AVAILABLE
            )
            if updated:
                logger.info(
                    "Seat %s released on flight %s after order %s cancellation",
                    seat_code, flight_id, order_id,
                )
            else:
                logger.info(
                    "Seat map key for flight %s does not exist in Redis, skipping seat %s release (will hydrate on next query)",
                    flight_id, seat_code,
                )
            return True
        except Exception:
            logger.error(
                "Failed to release seat %s on flight %s (attempt %d/%d)",
                seat_code, flight_id, attempt + 1, max_retries,
                exc_info=True,
            )
            if attempt < max_retries - 1:
                await asyncio.sleep(1 * (attempt + 1))

    logger.critical(
        "GHOST LOCK: Seat %s on flight %s is locked in Redis but order %s "
        "is cancelled in DB. Manual intervention required.",
        seat_code, flight_id, order_id,
    )
    return False


async def process_expired_orders(
    redis: aioredis.Redis,
    batch_size: int = 50,
    db: AsyncSession | None = None,
) -> int:
    now_ts = datetime.now(timezone.utc).timestamp()
    due_orders = await redis.eval(
        ZPOPMIN_BY_SCORE_SCRIPT, 1, ORDER_TIMEOUT_QUEUE_KEY, now_ts, batch_size
    )
    if not due_orders:
        return 0

    processed = 0
    for raw_task in due_orders:
        try:
            task = json.loads(raw_task)
            await cancel_expired_order(
                redis, task["order_id"], task["flight_id"], task["seat_code"], db=db
            )
            # Clear retry counter on success
            retry_key = f"order:timeout:retry:{task['order_id']}"
            await redis.delete(retry_key)
            processed += 1
        except Exception:
            logger.error(
                "Error processing expired order payload %s, attempting retry re-queue",
                raw_task,
                exc_info=True,
            )
            try:
                task = json.loads(raw_task)
                retry_key = f"order:timeout:retry:{task['order_id']}"
                retries = await redis.incr(retry_key)
                await redis.expire(retry_key, 300)
                if retries <= _ORDER_TIMEOUT_MAX_RETRIES:
                    retry_ts = datetime.now(timezone.utc).timestamp() + _ORDER_TIMEOUT_RETRY_DELAY
                    await redis.zadd(ORDER_TIMEOUT_QUEUE_KEY, {raw_task: retry_ts})
                    logger.warning(
                        "Order %s cancellation failed (attempt %d/%d), re-queued for retry in %ds",
                        task["order_id"],
                        retries,
                        _ORDER_TIMEOUT_MAX_RETRIES,
                        _ORDER_TIMEOUT_RETRY_DELAY,
                    )
                else:
                    logger.critical(
                        "Order %s cancellation exceeded max retries (%d). Dropping task to prevent poison pill.",
                        task["order_id"],
                        _ORDER_TIMEOUT_MAX_RETRIES,
                    )
            except Exception:
                logger.error("Failed to re-queue failed order task %s", raw_task, exc_info=True)

    return processed


async def run_timeout_worker(
    redis: aioredis.Redis,
    poll_interval: float = 1.0,
) -> None:
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


async def pay_order(
    db: AsyncSession,
    order_id: UUID,
    user_id: UUID,
    redis: aioredis.Redis | None = None,
) -> Order:
    order = (
        await db.execute(select(Order).where(Order.id == order_id))
    ).scalar_one_or_none()

    if order is None:
        raise OrderNotFoundException(f"Order {order_id} not found.")

    if order.user_id != user_id:
        raise OrderForbiddenException("You do not have permission to pay for this order.")

    # Update status only if still PENDING
    stmt = (
        update(Order)
        .where(Order.id == order_id, Order.status == OrderStatus.PENDING)
        .values(status=OrderStatus.CONFIRMED)
    )
    result = await db.execute(stmt)
    if result.rowcount == 0:
        raise InvalidOrderStatusException("Order already processed or expired.")

    seat_stmt = (
        update(Seat)
        .where(
            Seat.flight_id == order.flight_id,
            Seat.seat_code == order.seat_code,
        )
        .values(status=1)
    )
    await db.execute(seat_stmt)

    await db.commit()
    await db.refresh(order)
    logger.info("Order %s confirmed and paid by user %s", order_id, user_id)

    # Purge task from timeout queue
    if redis is not None:
        order_task = json.dumps({
            "order_id": str(order.id),
            "flight_id": str(order.flight_id),
            "seat_code": order.seat_code,
        })
        try:
            await redis.zrem(ORDER_TIMEOUT_QUEUE_KEY, order_task)
            await redis.delete(f"order:timeout:retry:{order.id}")
            await _hset_seat_if_exists(redis, order.flight_id, order.seat_code, SeatState.LOCKED)
            logger.info("Order %s paid; timeout queue cleared and seat confirmed locked in Redis", order.id)
        except Exception:
            logger.warning(
                "Failed to remove paid order %s from timeout queue", order.id, exc_info=True
            )

    return order


async def cancel_order_by_user(
    db: AsyncSession,
    order_id: UUID,
    user_id: UUID,
    redis: aioredis.Redis | None = None,
) -> Order:
    stmt = select(Order).where(Order.id == order_id)
    order = (await db.execute(stmt)).scalar_one_or_none()
    if not order:
        raise OrderNotFoundException(order_id)

    if order.user_id != user_id:
        raise OrderForbiddenException("You do not have permission to cancel this order.")

    stmt = (
        update(Order)
        .where(Order.id == order_id, Order.status == OrderStatus.PENDING)
        .values(status=OrderStatus.CANCELLED)
    )
    result = await db.execute(stmt)
    if result.rowcount == 0:
        raise InvalidOrderStatusException("Order already processed or expired.")

    await db.commit()
    await db.refresh(order)
    logger.info("Order %s cancelled by user %s", order_id, user_id)

    if redis is not None:
        order_task = json.dumps({
            "order_id": str(order.id),
            "flight_id": str(order.flight_id),
            "seat_code": order.seat_code,
        })
        try:
            await redis.zrem(ORDER_TIMEOUT_QUEUE_KEY, order_task)
            await redis.delete(f"order:timeout:retry:{order.id}")
        except Exception:
            logger.warning(
                "Failed to purge timeout queue for order %s", order.id, exc_info=True
            )
        await release_seat_with_retry(redis, order.flight_id, order.seat_code, order.id)

    return order
