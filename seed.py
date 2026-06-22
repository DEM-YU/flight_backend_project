

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import redis.asyncio as aioredis
from sqlalchemy import select

from database import AsyncSessionLocal, engine, settings
from models import Base, Flight, Seat, SeatState, User
from auth import hash_password


FLIGHT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
USER_ID = uuid.UUID("00000000-0000-0000-0000-000000000002")
USER_EMAIL = "test@example.com"
USER_PASSWORD = "password123"
SEAT_COUNT = 30
SEAT_DB_STATUS_AVAILABLE = 0


async def main() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("[INFO] Tables created.")

    async with AsyncSessionLocal() as db:
        # Check and seed test user
        existing_user = await db.execute(select(User).where(User.id == USER_ID))
        if existing_user.scalar_one_or_none():
            print("[WARN] Test user already exists, skipping user seed.")
        else:
            hashed_pw = hash_password(USER_PASSWORD)
            test_user = User(
                id=USER_ID,
                email=USER_EMAIL,
                hashed_password=hashed_pw,
            )
            db.add(test_user)
            await db.commit()
            print(f"[INFO] Test user {USER_EMAIL} inserted.")

        existing = await db.execute(select(Flight).where(Flight.id == FLIGHT_ID))
        if existing.scalar_one_or_none():
            print("[WARN] Test flight already exists, skipping.")
        else:
            flight = Flight(
                id=FLIGHT_ID,
                flight_num="TEST-001",
                departure="PEK",
                arrival="SHA",
                # Dynamically set 30 days from now so the flight is always in the future.
                departure_time=datetime.now(timezone.utc) + timedelta(days=30),
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
