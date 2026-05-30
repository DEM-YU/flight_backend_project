"""
seed.py — Initialize test data for the application.

Execution order:
  1. Create tables (CREATE TABLE IF NOT EXISTS)
  2. Seed a test flight record
  3. Seed 30 seat records (1A ~ 30A)
  4. Initialize seat status in Redis Hash (0 = Available)

Usage:
  python seed.py
"""

import asyncio
import uuid
from datetime import datetime, timezone

import redis.asyncio as aioredis
from sqlalchemy import text

from database import AsyncSessionLocal, engine, settings
from models import Base, Flight, Seat

# ── Fixed flight_id for compatibility with the stress testing script ──
FLIGHT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
SEAT_COUNT = 30


async def main() -> None:
    # 1. Create database schema
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("✅ Database tables created successfully")

    async with AsyncSessionLocal() as db:
        # 2. Check if the test flight already exists to avoid duplicate seeding
        from sqlalchemy import select
        existing = await db.execute(select(Flight).where(Flight.id == FLIGHT_ID))
        if existing.scalar_one_or_none():
            print("⚠️  Test flight already exists, skipping database seed")
        else:
            # Insert flight
            flight = Flight(
                id=FLIGHT_ID,
                flight_num="TEST-001",
                departure="PEK",
                arrival="SHA",
                departure_time=datetime(2026, 6, 1, 8, 0, tzinfo=timezone.utc),
            )
            db.add(flight)

            # Insert seats
            seats = [
                Seat(
                    id=uuid.uuid4(),
                    flight_id=FLIGHT_ID,
                    seat_code=f"{i}A",
                    status=0,
                )
                for i in range(1, SEAT_COUNT + 1)
            ]
            db.add_all(seats)
            await db.commit()
            print(f"✅ Flight {FLIGHT_ID} and {SEAT_COUNT} seats seeded in PostgreSQL")

    # 3. Seed initial seat inventory status in Redis Hash
    redis = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    seat_key = f"flight:{FLIGHT_ID}:seats"

    # Batch write: HSET flight:<id>:seats 1A 0 2A 0 ... 30A 0
    seat_map = {f"{i}A": "0" for i in range(1, SEAT_COUNT + 1)}
    await redis.hset(seat_key, mapping=seat_map)
    await redis.aclose()
    print(f"✅ Seeded {SEAT_COUNT} seat statuses in Redis Hash [{seat_key}]")

    print("\n🎉 Seed data initialization complete! Ready for stress testing.")


if __name__ == "__main__":
    asyncio.run(main())
