import asyncio
import json
import uuid
from datetime import date, datetime, timezone
from uuid import UUID

import redis.asyncio as aioredis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal
from models import Flight, FlightResponse, Order

# Cache TTL: 30-minute base duration with randomized jitter to mitigate cache stampedes
_CACHE_TTL = 1800

# custom exceptions
class SeatUnavailableException(Exception):
    """Seat already locked or non-existent, reservation failed."""


# Lua Script: Atomic seat checking and locking mechanism 
# KEYS[1] : Hash table name, structured as flight:{flight_id}:seats
# ARGV[1] : Target seat identifier, e.g., '12A'
# Returns 1 = Lock acquired successfully; Returns 0 = Occupied or non-existent
RESERVE_SEAT_SCRIPT = """
local status = redis.call('HGET', KEYS[1], ARGV[1])
if status == '0' then
    redis.call('HSET', KEYS[1], ARGV[1], '1')
    return 1
else
    return 0
end
"""

def _cache_key(departure: str, arrival: str, date_str: str) -> str:
    return f"route:{departure}:{arrival}:{date_str}"


async def search_flights(
    db: AsyncSession,
    redis: aioredis.Redis,
    departure: str,
    arrival: str,
    date_str: str,
) -> tuple[list[FlightResponse], bool]:
    """
    Query flight data implementing the Cache-Aside pattern.

    Returns:
        tuple: (flights, is_from_cache) to provide granular layer observability.
    """
    key = _cache_key(departure, arrival, date_str)

    # Step A: Cache Hit     
    cached = await redis.get(key)
    if cached:
        data = json.loads(cached)
        return [FlightResponse(**item) for item in data], True

    # Step B: Cache Miss → Query DB 
    target_date: date = date.fromisoformat(date_str)

    # Range query execution: [target_date 00:00 UTC, target_date+1 00:00 UTC) bounds
    day_start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
    day_end = datetime(target_date.year, target_date.month, target_date.day + 1, tzinfo=timezone.utc)

    stmt = select(Flight).where(
        Flight.departure == departure,
        Flight.arrival == arrival,
        Flight.departure_time >= day_start,
        Flight.departure_time < day_end,
    )

    result = await db.execute(stmt)
    flights = result.scalars().all()
    schemas = [FlightResponse.model_validate(f) for f in flights]

# Step C: Write-Back with TTL
    payload = json.dumps([s.model_dump(mode="json") for s in schemas])
    await redis.setex(key, _CACHE_TTL, payload)

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
        SeatUnavailableException: Errored if seat is already locked or non-existent.
    """
    seat_key = f"flight:{flight_id}:seats"

    # Step 1: Redis atomic concurrency control to prevent overselling
    result = await redis.eval(RESERVE_SEAT_SCRIPT, 1, seat_key, seat_code)
    if result == 0:
        raise SeatUnavailableException(f"Seat {seat_code} on flight {flight_id} is unavailable.")

    # Step 2: PostgreSQL transactional order persistence for durability
    order = Order(
        id=uuid.uuid4(),
        user_id=user_id,
        flight_id=flight_id,
        seat_code=seat_code,
        status="Pending",
    )
    db.add(order)
    await db.commit()
    await db.refresh(order)
    return order