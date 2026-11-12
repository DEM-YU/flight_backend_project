import json
import uuid
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import select

from auth import hash_password
from models import Flight, Order, OrderStatus, Seat, SeatState, User
from services import (
    ORDER_TIMEOUT_QUEUE_KEY,
    AsyncSessionLocal,
    FlightNotFoundException,
    InvalidOrderStatusException,
    OrderForbiddenException,
    OrderNotFoundException,
    SeatNotFoundException,
    SeatUnavailableException,
    cancel_expired_order,
    get_flight_seat_map,
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


class TestFlightSeatMap:
    async def test_get_seat_map_cache_miss_hydrates_from_postgres_and_populates_redis(
        self, db_session, redis_client
    ):
        """Verify Cache-Aside: When Redis is empty, query loads seats from PostgreSQL and populates Redis."""
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "1A", status=0)
        await _create_test_seat(db_session, flight.id, "1B", status=0)

        seat_key = f"flight:{flight.id}:seats"
        # Ensure Redis has no cache initially
        await redis_client.delete(seat_key)

        # 1. Query seat map -> triggers cache miss and auto-hydrates from PostgreSQL
        seat_map = await get_flight_seat_map(db_session, redis_client, flight.id)
        assert seat_map.flight_id == flight.id
        assert seat_map.total_seats == 2
        assert seat_map.available_seats == 2
        assert [s.seat_code for s in seat_map.seats] == ["1A", "1B"]
        assert all(s.is_available for s in seat_map.seats)

        # 2. Verify Redis Hash was physically populated
        cached_seats = await redis_client.hgetall(seat_key)
        assert cached_seats == {"1A": "0", "1B": "0"}

    async def test_get_seat_map_reflects_locked_seats(self, db_session, redis_client):
        """Verify seat map returns accurate availability when a seat is locked in Redis."""
        flight = await _create_test_flight(db_session)
        seat_key = f"flight:{flight.id}:seats"
        await redis_client.hset(seat_key, mapping={"1A": "1", "1B": "0"})

        seat_map = await get_flight_seat_map(db_session, redis_client, flight.id)
        assert seat_map.total_seats == 2
        assert seat_map.available_seats == 1

        seat_1a = next(s for s in seat_map.seats if s.seat_code == "1A")
        seat_1b = next(s for s in seat_map.seats if s.seat_code == "1B")
        assert seat_1a.is_available is False
        assert seat_1a.status == 1
        assert seat_1b.is_available is True
        assert seat_1b.status == 0

    async def test_get_seat_map_non_existent_flight_raises_404(self, db_session, redis_client):
        fake_id = uuid.uuid4()
        with pytest.raises(FlightNotFoundException):
            await get_flight_seat_map(db_session, redis_client, fake_id)

    async def test_reserve_seat_auto_hydrates_when_redis_empty(self, db_session, redis_client):
        """Verify Cache-Aside on reserve: Even if Redis is blank, reserve_seat loads DB seats and succeeds."""
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "2C", status=0)

        seat_key = f"flight:{flight.id}:seats"
        # Make sure Redis has no key
        await redis_client.delete(seat_key)

        # Reserve seat without prior seeding in Redis -> auto-hydrates and succeeds
        order = await reserve_seat(db_session, redis_client, user.id, flight.id, "2C")
        assert order.status == OrderStatus.PENDING
        assert order.seat_code == "2C"

        # Verify Redis now has the seat locked
        status_in_redis = await redis_client.hget(seat_key, "2C")
        assert status_in_redis == SeatState.LOCKED

    async def test_api_get_seats_endpoint_success(self, async_client, db_session, redis_client):
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "3A", status=0)

        resp = await async_client.get(f"/api/v1/flights/{flight.id}/seats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["flight_id"] == str(flight.id)
        assert data["total_seats"] == 1
        assert data["available_seats"] == 1
        assert data["seats"][0]["seat_code"] == "3A"
        assert data["seats"][0]["is_available"] is True


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

    async def test_database_partial_unique_index_prevents_duplicate_active_orders(
        self, db_session, redis_client
    ):
        """Defensive Baseline: If Redis desyncs, PostgreSQL unique constraint catches duplicate active orders."""
        user1 = await _create_test_user(db_session)
        user2 = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "5A")

        seat_key = f"flight:{flight.id}:seats"
        await redis_client.hset(seat_key, "5A", SeatState.AVAILABLE)

        # 1. First user reserves seat 5A -> succeeds in Redis and PostgreSQL
        order1 = await reserve_seat(db_session, redis_client, user1.id, flight.id, "5A")
        assert order1.status == OrderStatus.PENDING

        # 2. Simulate Redis desync / split-brain: Redis seat is mistakenly reset to AVAILABLE
        await redis_client.hset(seat_key, "5A", SeatState.AVAILABLE)

        # 3. Second user attempts to reserve the same seat:
        # Redis Lua check passes, but PostgreSQL partial unique index blocks the insert
        with pytest.raises(SeatUnavailableException):
            await reserve_seat(db_session, redis_client, user2.id, flight.id, "5A")

        # 4. Redis seat must be corrected back to LOCKED to prevent further invalid attempts
        seat_status = await redis_client.hget(seat_key, "5A")
        assert seat_status == SeatState.LOCKED


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

    async def test_pay_order_removes_task_from_timeout_queue(self, db_session, redis_client):
        """Verify Resource Cleanup: Paying an order proactively deletes it from Redis ZSet timeout queue."""
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "14B", status=0)

        order = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="14B",
            status=OrderStatus.PENDING,
        )
        db_session.add(order)
        await db_session.commit()

        # 1. Schedule order timeout in Redis ZSet delay queue
        await schedule_order_timeout(redis_client, str(order.id), str(flight.id), "14B")

        # Verify task exists in ZSet
        initial_count = await redis_client.zcard(ORDER_TIMEOUT_QUEUE_KEY)
        assert initial_count >= 1

        # 2. Pay order passing redis client
        await pay_order(db_session, order.id, user.id, redis=redis_client)

        # 3. Verify task was physically deleted from Redis ZSet queue
        task_payload = json.dumps({
            "order_id": str(order.id),
            "flight_id": str(flight.id),
            "seat_code": "14B",
        })
        score = await redis_client.zscore(ORDER_TIMEOUT_QUEUE_KEY, task_payload)
        assert score is None


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


class TestPayAndCancelConcurrency:
    async def test_worker_skips_seat_release_if_order_already_paid(self, redis_client):
        """Verify atomic CAS: If user paid, timeout worker must not release Redis seat."""
        async with AsyncSessionLocal() as db:
            user = await _create_test_user(db)
            flight = await _create_test_flight(db)
            seat = await _create_test_seat(db, flight.id, "10A", status=0)
            order = Order(
                id=uuid.uuid4(),
                user_id=user.id,
                flight_id=flight.id,
                seat_code="10A",
                status=OrderStatus.PENDING,
            )
            db.add(order)
            await db.commit()

        seat_key = f"flight:{flight.id}:seats"
        await redis_client.hset(seat_key, "10A", SeatState.LOCKED)

        try:
            # 1. User pays the order -> status becomes CONFIRMED
            async with AsyncSessionLocal() as db:
                await pay_order(db, order.id, user.id)

            # 2. Worker attempts to cancel -> CAS fails because status is not PENDING
            cancelled = await cancel_expired_order(
                redis_client, str(order.id), str(flight.id), "10A"
            )
            assert cancelled is False

            # 3. Redis seat must REMAIN LOCKED, NOT released to AVAILABLE ('0')
            redis_seat_status = await redis_client.hget(seat_key, "10A")
            assert redis_seat_status == SeatState.LOCKED
        finally:
            from sqlalchemy import delete
            async with AsyncSessionLocal() as db:
                await db.execute(delete(Order).where(Order.id == order.id))
                await db.execute(delete(Seat).where(Seat.flight_id == flight.id))
                await db.execute(delete(Flight).where(Flight.id == flight.id))
                await db.execute(delete(User).where(User.id == user.id))
                await db.commit()

    async def test_pay_fails_if_worker_already_cancelled_order(self, redis_client):
        """Verify atomic CAS: If timeout worker cancelled order, payment attempt must fail."""
        async with AsyncSessionLocal() as db:
            user = await _create_test_user(db)
            flight = await _create_test_flight(db)
            seat = await _create_test_seat(db, flight.id, "11B", status=0)
            order = Order(
                id=uuid.uuid4(),
                user_id=user.id,
                flight_id=flight.id,
                seat_code="11B",
                status=OrderStatus.PENDING,
            )
            db.add(order)
            await db.commit()

        seat_key = f"flight:{flight.id}:seats"
        await redis_client.hset(seat_key, "11B", SeatState.LOCKED)

        try:
            # 1. Worker auto-cancels the order -> status becomes CANCELLED
            cancelled = await cancel_expired_order(
                redis_client, str(order.id), str(flight.id), "11B"
            )
            assert cancelled is True

            # 2. Redis seat must be released to AVAILABLE ('0')
            redis_seat_status = await redis_client.hget(seat_key, "11B")
            assert redis_seat_status == SeatState.AVAILABLE

            # 3. User attempts to pay afterwards -> atomic CAS fails and raises exception
            async with AsyncSessionLocal() as db:
                with pytest.raises(InvalidOrderStatusException):
                    await pay_order(db, order.id, user.id)
        finally:
            from sqlalchemy import delete
            async with AsyncSessionLocal() as db:
                await db.execute(delete(Order).where(Order.id == order.id))
                await db.execute(delete(Seat).where(Seat.flight_id == flight.id))
                await db.execute(delete(Flight).where(Flight.id == flight.id))
                await db.execute(delete(User).where(User.id == user.id))
                await db.commit()
