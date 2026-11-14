

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
USER_COUNT = 1000
SEAT_COUNT = 100
SEAT_DB_STATUS_AVAILABLE = 0


async def main() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("[INFO] Tables created.")

    async with AsyncSessionLocal() as db:
        hashed_pw = hash_password(USER_PASSWORD)

        # Seed primary test user if missing
        existing_user = await db.execute(select(User).where(User.id == USER_ID))
        if not existing_user.scalar_one_or_none():
            test_user = User(
                id=USER_ID,
                email=USER_EMAIL,
                hashed_password=hashed_pw,
            )
            db.add(test_user)
            await db.commit()
            print(f"[INFO] Test user {USER_EMAIL} inserted.")

        # Seed concurrent pool users (user_0001 to user_1000) for authentic stress testing
        existing_emails = set((await db.execute(select(User.email))).scalars().all())
        users_to_add = []
        for i in range(1, USER_COUNT + 1):
            email = f"user_{i:04d}@example.com"
            if email not in existing_emails:
                users_to_add.append(
                    User(
                        id=uuid.UUID(f"00000000-0000-0000-0001-{i:012d}"),
                        email=email,
                        hashed_password=hashed_pw,
                    )
                )
        if users_to_add:
            db.add_all(users_to_add)
            await db.commit()
            print(f"[INFO] Inserted {len(users_to_add)} test pool users for stress testing.")
        else:
            print(f"[INFO] {USER_COUNT} test pool users already exist.")

        # Seed flight and seats
        existing_flight = await db.execute(select(Flight).where(Flight.id == FLIGHT_ID))
        if not existing_flight.scalar_one_or_none():
            flight = Flight(
                id=FLIGHT_ID,
                flight_num="TEST-001",
                departure="PEK",
                arrival="SHA",
                departure_time=datetime.now(timezone.utc) + timedelta(days=30),
            )
            db.add(flight)
            await db.commit()
            print(f"[INFO] Flight {FLIGHT_ID} inserted.")

        existing_seats = set((await db.execute(select(Seat.seat_code).where(Seat.flight_id == FLIGHT_ID))).scalars().all())
        seats_to_add = []
        for i in range(1, SEAT_COUNT + 1):
            code = f"{i}A"
            if code not in existing_seats:
                seats_to_add.append(
                    Seat(
                        id=uuid.uuid4(),
                        flight_id=FLIGHT_ID,
                        seat_code=code,
                        status=SEAT_DB_STATUS_AVAILABLE,
                    )
                )
        if seats_to_add:
            db.add_all(seats_to_add)
            await db.commit()
            print(f"[INFO] Inserted {len(seats_to_add)} seats for flight {FLIGHT_ID}.")
        else:
            print(f"[INFO] {SEAT_COUNT} seats already exist for flight {FLIGHT_ID}.")

    redis = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    seat_key = f"flight:{FLIGHT_ID}:seats"

    seat_map = {f"{i}A": SeatState.AVAILABLE for i in range(1, SEAT_COUNT + 1)}
    await redis.hset(seat_key, mapping=seat_map)
    await redis.aclose()
    print(f"[INFO] {SEAT_COUNT} seats seeded in Redis [{seat_key}].")
    print("[INFO] Seed complete.")


if __name__ == "__main__":
    asyncio.run(main())
