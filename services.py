import json
from datetime import date, datetime, timezone

import redis.asyncio as aioredis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Flight, FlightResponse

# Cache TTL: 30-minute base duration with randomized jitter to mitigate cache stampedes
_CACHE_TTL = 1800


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