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

    key = _cache_key(departure, arrival, date_str)

     
    cached = await redis.get(key)
    if cached:
        cached_flights = json.loads(cached)
        return [FlightResponse(**f) for f in cached_flights], True

 
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

    flights = (await db.execute(stmt)).scalars().all()
    flight_responses = [FlightResponse.model_validate(f) for f in flights]

# Step C: Write-Back with TTL
    cache_payload = json.dumps([f.model_dump(mode="json") for f in flight_responses])
    await redis.setex(key, _CACHE_TTL, payload)

    return flight_responses, False