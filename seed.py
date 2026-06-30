

import asyncio
import uuid
from datetime import datetime, timezone

import redis.asyncio as aioredis


from database import AsyncSessionLocal, engine, settings
from models import Base, Flight, Seat, SeatState


FLIGHT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
SEAT_COUNT = 30
SEAT_DB_STATUS_AVAILABLE = 0


async def main() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("[INFO] Tables created.")

    async with AsyncSessionLocal() as db:
        from sqlalchemy import select
        existing = await db.execute(select(Flight).where(Flight.id == FLIGHT_ID))
        if existing.scalar_one_or_none():
            print("[WARN] Test flight already exists, skipping.")
        else:
            flight = Flight(
                id=FLIGHT_ID,
                flight_num="TEST-001",
                departure="PEK",
                arrival="SHA",
                departure_time=datetime(2026, 6, 1, 8, 0, tzinfo=timezone.utc),
            )
            db.add(flight)

            seats = [
                Seat(
                    id=uuid.uuid4(),
                    flight_id=FLIGHT_ID,
                    seat_code=f"{i}A",
                    status=SEAT_DB_STATUS_AVAILABLE,
                )
                for i in range(1, SEAT_COUNT + 1)
            ]
            db.add_all(seats)
            await db.commit()
            print(f"[INFO] Flight {FLIGHT_ID} and {SEAT_COUNT} seats inserted.")

    redis = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    seat_key = f"flight:{FLIGHT_ID}:seats"

    seat_map = {f"{i}A": SeatState.AVAILABLE for i in range(1, SEAT_COUNT + 1)}
    await redis.hset(seat_key, mapping=seat_map)
    await redis.aclose()
    print(f"[INFO] {SEAT_COUNT} seats seeded in Redis [{seat_key}].")
    print("[INFO] Seed complete.")


if __name__ == "__main__":
    asyncio.run(main())
