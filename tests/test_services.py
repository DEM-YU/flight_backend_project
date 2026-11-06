import json
import uuid
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import select

from auth import hash_password
from models import Flight, Order, OrderStatus, Seat, SeatState, User
from services import (
    ORDER_TIMEOUT_QUEUE_KEY,
    InvalidOrderStatusException,
    OrderForbiddenException,
    OrderNotFoundException,
    SeatNotFoundException,
    SeatUnavailableException,
    pay_order,
    process_expired_orders,
    reserve_seat,
    schedule_order_timeout,
    search_flights,
)

pytestmark = pytest.mark.asyncio


async def _create_test_flight(db, departure="PEK", arrival="SHA", flight_date=None) -> Flight:
    if flight_date is None:
        flight_date = date.today()
    dep_time = datetime(flight_date.year, flight_date.month, flight_date.day, 10, 0, tzinfo=timezone.utc)
    flight = Flight(
        id=uuid.uuid4(),
        flight_num=f"FL-{uuid.uuid4().hex[:4].upper()}",
        departure=departure,
        arrival=arrival,
        departure_time=dep_time,
    )
    db.add(flight)
    await db.commit()
    await db.refresh(flight)
    return flight


async def _create_test_user(db, email=None) -> User:
    if email is None:
        email = f"user_{uuid.uuid4().hex[:8]}@example.com"
    user = User(
        id=uuid.uuid4(),
        email=email,
        hashed_password=hash_password("password123"),
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user


async def _create_test_seat(db, flight_id: uuid.UUID, seat_code="1A", status=0) -> Seat:
    seat = Seat(
        id=uuid.uuid4(),
        flight_id=flight_id,
        seat_code=seat_code,
        status=status,
    )
    db.add(seat)
    await db.commit()
    await db.refresh(seat)
    return seat


class TestSearchFlights:
    async def test_cache_miss_queries_db_and_writes_to_redis(self, db_session, redis_client):
        today = date.today()
        flight = await _create_test_flight(db_session, departure="PEK", arrival="SHA", flight_date=today)

        # 1. First query: cache miss -> queries PostgreSQL and caches in Redis
        flights, is_cache_hit = await search_flights(db_session, redis_client, "PEK", "SHA", today)
        assert is_cache_hit is False
        assert any(f.id == flight.id for f in flights)

        # Verify key was physically written to Redis
        cache_key = f"route:PEK:SHA:{today.isoformat()}"
        cached_data = await redis_client.get(cache_key)
        assert cached_data is not None

        # 2. Second query: cache hit directly from Redis
        flights_cached, is_cache_hit_2 = await search_flights(db_session, redis_client, "PEK", "SHA", today)
        assert is_cache_hit_2 is True
        assert any(f.id == flight.id for f in flights_cached)

    async def test_empty_route_uses_short_negative_cache_ttl(self, db_session, redis_client):
        today = date.today()
        # Non-existent route
        results, is_cache_hit = await search_flights(db_session, redis_client, "NON", "EXIST", today)
        assert results == []
        assert is_cache_hit is False

        # Verify negative cache was set with short TTL (<= 60s)
        cache_key = f"route:NON:EXIST:{today.isoformat()}"
        ttl = await redis_client.ttl(cache_key)
        assert 0 < ttl <= 60


class TestReserveSeat:
    async def test_successful_reservation_locks_redis_and_persists_order(self, db_session, redis_client):
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "1A")

        # Seed Redis seat hash as available ('0')
        seat_key = f"flight:{flight.id}:seats"
        await redis_client.hset(seat_key, "1A", SeatState.AVAILABLE)

        # Execute reservation
        order = await reserve_seat(db_session, redis_client, user.id, flight.id, "1A")
        assert order.status == OrderStatus.PENDING
        assert order.seat_code == "1A"
        assert order.user_id == user.id

        # Verify Redis seat state was atomically updated to '1' (LOCKED)
        status_in_redis = await redis_client.hget(seat_key, "1A")
        assert status_in_redis == SeatState.LOCKED

        # Verify Order physically exists in PostgreSQL
        stmt = select(Order).where(Order.id == order.id)
        saved_order = (await db_session.execute(stmt)).scalar_one_or_none()
        assert saved_order is not None
        assert saved_order.status == OrderStatus.PENDING

    async def test_seat_already_locked_raises_unavailable(self, db_session, redis_client):
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)

        # Seat is already locked ('1') in Redis
        seat_key = f"flight:{flight.id}:seats"
        await redis_client.hset(seat_key, "2B", SeatState.LOCKED)

        with pytest.raises(SeatUnavailableException):
            await reserve_seat(db_session, redis_client, user.id, flight.id, "2B")

    async def test_uninitialized_seat_raises_not_found(self, db_session, redis_client):
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)

        # Seat does not exist in Redis Hash
        with pytest.raises(SeatNotFoundException):
            await reserve_seat(db_session, redis_client, user.id, flight.id, "99Z")

    async def test_db_failure_triggers_compensating_redis_unlock(self, db_session, redis_client):
        flight = await _create_test_flight(db_session)
        seat_key = f"flight:{flight.id}:seats"
        await redis_client.hset(seat_key, "3C", SeatState.AVAILABLE)

        # Non-existent user_id violates PostgreSQL foreign key constraint
        fake_user_id = uuid.uuid4()

        with pytest.raises(Exception):
            await reserve_seat(db_session, redis_client, fake_user_id, flight.id, "3C")

        # Redis seat lock MUST be rolled back to '0' to prevent ghost locks
        status_after_rollback = await redis_client.hget(seat_key, "3C")
        assert status_after_rollback == SeatState.AVAILABLE


class TestRedisZSetTimeoutQueue:
    async def test_schedule_and_process_expired_orders(self, redis_client):
        from database import AsyncSessionLocal

        async with AsyncSessionLocal() as db:
            user = await _create_test_user(db)
            flight = await _create_test_flight(db)
            seat = await _create_test_seat(db, flight.id, "7D")

            order = Order(
                id=uuid.uuid4(),
                user_id=user.id,
                flight_id=flight.id,
                seat_code="7D",
                status=OrderStatus.PENDING,
            )
            db.add(order)
            await db.commit()

        try:
            # Lock seat in Redis
            seat_key = f"flight:{flight.id}:seats"
            await redis_client.hset(seat_key, "7D", SeatState.LOCKED)

            # Schedule timeout with delay=-1 so score < now (immediately due)
            await schedule_order_timeout(redis_client, str(order.id), str(flight.id), "7D", delay=-1)

            # Verify entry exists in ZSet
            zcount = await redis_client.zcard(ORDER_TIMEOUT_QUEUE_KEY)
            assert zcount >= 1

            # Process due orders using Redis Lua script and cancel_expired_order worker
            processed_count = await process_expired_orders(redis_client)
            assert processed_count >= 1

            # Redis seat lock must be released back to '0'
            seat_state = await redis_client.hget(seat_key, "7D")
            assert seat_state == SeatState.AVAILABLE

            # Database order status must be cancelled
            async with AsyncSessionLocal() as db:
                res = await db.execute(select(Order).where(Order.id == order.id))
                refreshed_order = res.scalar_one_or_none()
                assert refreshed_order is not None
                assert refreshed_order.status == OrderStatus.CANCELLED
        finally:
            from sqlalchemy import delete
            async with AsyncSessionLocal() as db:
                await db.execute(delete(Order).where(Order.id == order.id))
                await db.execute(delete(Seat).where(Seat.flight_id == flight.id))
                await db.execute(delete(Flight).where(Flight.id == flight.id))
                await db.execute(delete(User).where(User.id == user.id))
                await db.commit()


class TestPayOrder:
    async def test_pay_order_success(self, db_session):
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        seat = await _create_test_seat(db_session, flight.id, "12A", status=0)

        order = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="12A",
            status=OrderStatus.PENDING,
        )
        db_session.add(order)
        await db_session.commit()

        # Pay order
        confirmed_order = await pay_order(db_session, order.id, user.id)
        assert confirmed_order.status == OrderStatus.CONFIRMED

        # Seat in PostgreSQL must be updated to occupied (1)
        await db_session.refresh(seat)
        assert seat.status == 1

    async def test_pay_order_not_found_raises(self, db_session):
        user = await _create_test_user(db_session)
        with pytest.raises(OrderNotFoundException):
            await pay_order(db_session, uuid.uuid4(), user.id)

    async def test_pay_order_forbidden_for_other_user(self, db_session):
        owner = await _create_test_user(db_session)
        other_user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "4D")

        order = Order(
            id=uuid.uuid4(),
            user_id=owner.id,
            flight_id=flight.id,
            seat_code="4D",
            status=OrderStatus.PENDING,
        )
        db_session.add(order)
        await db_session.commit()

        with pytest.raises(OrderForbiddenException):
            await pay_order(db_session, order.id, other_user.id)

    async def test_pay_order_already_cancelled_raises(self, db_session):
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "5E")

        order = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="5E",
            status=OrderStatus.CANCELLED,
        )
        db_session.add(order)
        await db_session.commit()

        with pytest.raises(InvalidOrderStatusException):
            await pay_order(db_session, order.id, user.id)

    async def test_pay_order_already_confirmed_raises(self, db_session):
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "6F")

        order = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="6F",
            status=OrderStatus.CONFIRMED,
        )
        db_session.add(order)
        await db_session.commit()

        with pytest.raises(InvalidOrderStatusException):
            await pay_order(db_session, order.id, user.id)


class TestPayOrderAPI:
    async def test_pay_endpoint_success(self, async_client, db_session):
        import jwt
        from database import settings

        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "8A", status=0)

        order = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="8A",
            status=OrderStatus.PENDING,
        )
        db_session.add(order)
        await db_session.commit()

        token = jwt.encode(
            {"sub": str(user.id), "type": "access"},
            settings.JWT_SECRET_KEY,
            algorithm=settings.JWT_ALGORITHM,
        )
        headers = {"Authorization": f"Bearer {token}"}

        resp = await async_client.post(f"/api/v1/orders/{order.id}/pay", headers=headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "Confirmed"
        assert data["id"] == str(order.id)
