import json
import uuid
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, patch

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
    cancel_order_by_user,
    get_flight_seat_map,
    pay_order,
    process_expired_orders,
    release_seat_with_retry,
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

    async def test_get_seat_map_hydrates_pending_and_confirmed_orders_from_db(self, db_session, redis_client):
        """Verify Cache-Aside Hydration: Active Pending and Confirmed orders are hydrated as LOCKED."""
        flight = await _create_test_flight(db_session)
        user = await _create_test_user(db_session)
        await _create_test_seat(db_session, flight.id, "3A", status=0)
        await _create_test_seat(db_session, flight.id, "3B", status=0)
        await _create_test_seat(db_session, flight.id, "3C", status=0)

        # 3A has an active PENDING order (seat table still says status=0)
        order_pending = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="3A",
            status=OrderStatus.PENDING,
        )
        # 3B has an active CONFIRMED order
        order_confirmed = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="3B",
            status=OrderStatus.CONFIRMED,
        )
        db_session.add(order_pending)
        db_session.add(order_confirmed)
        await db_session.commit()

        # Wipe Redis cache to simulate cache eviction / cold start
        seat_key = f"flight:{flight.id}:seats"
        await redis_client.delete(seat_key)

        seat_map = await get_flight_seat_map(db_session, redis_client, flight.id)
        assert seat_map.total_seats == 3
        assert seat_map.available_seats == 1

        seat_3a = next(s for s in seat_map.seats if s.seat_code == "3A")
        seat_3b = next(s for s in seat_map.seats if s.seat_code == "3B")
        seat_3c = next(s for s in seat_map.seats if s.seat_code == "3C")

        assert seat_3a.is_available is False
        assert seat_3a.status == 1
        assert seat_3b.is_available is False
        assert seat_3b.status == 1
        assert seat_3c.is_available is True
        assert seat_3c.status == 0

        # Verify Redis was written correctly
        cached = await redis_client.hgetall(seat_key)
        assert cached == {"3A": "1", "3B": "1", "3C": "0"}

    async def test_get_seat_map_hydration_preserves_concurrently_locked_seat_via_hsetnx(self, db_session, redis_client):
        """Verify HSETNX: Hydration does not overwrite a seat that was concurrently locked in Redis."""
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "4A", status=0)
        await _create_test_seat(db_session, flight.id, "4B", status=0)

        seat_key = f"flight:{flight.id}:seats"
        # Redis starts empty (cache miss)
        await redis_client.delete(seat_key)

        # Simulate race condition: Thread B locks 4A concurrently while Thread A is hydrating
        real_eval = redis_client.eval
        async def race_eval(script, numkeys, *keys_and_args):
            # Concurrent reservation by Thread B happens right before hydration writes
            await redis_client.hset(seat_key, "4A", SeatState.LOCKED)
            return await real_eval(script, numkeys, *keys_and_args)

        with patch.object(redis_client, "eval", side_effect=race_eval):
            seat_map = await get_flight_seat_map(db_session, redis_client, flight.id)

        # 4A must still be LOCKED ('1') in Redis, NOT overwritten by DB's status=0
        cached = await redis_client.hgetall(seat_key)
        assert cached["4A"] == "1"
        assert cached["4B"] == "0"

        seat_4a = next(s for s in seat_map.seats if s.seat_code == "4A")
        assert seat_4a.is_available is False
        assert seat_4a.status == 1

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

    async def test_reserve_seat_non_active_order_unique_error_rolls_back_lock(
        self, db_session, redis_client
    ):
        """Verify precision of unique constraint check:
        Only uq_flight_seat_active_order should mark Redis as LOCKED.
        Other integrity errors (even containing the word 'unique') must rollback Redis lock to AVAILABLE."""
        from sqlalchemy.exc import IntegrityError

        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "8C")

        seat_key = f"flight:{flight.id}:seats"
        await redis_client.hset(seat_key, "8C", SeatState.AVAILABLE)

        with patch.object(
            db_session,
            "commit",
            side_effect=IntegrityError("duplicate key value violates unique constraint 'users_email_key'", params={}, orig=Exception()),
        ):
            with pytest.raises(IntegrityError):
                await reserve_seat(db_session, redis_client, user.id, flight.id, "8C")

        # Must NOT lock Redis to '1'; it must rollback Redis lock to AVAILABLE ('0')
        seat_status = await redis_client.hget(seat_key, "8C")
        assert seat_status == SeatState.AVAILABLE

    async def test_reserve_seat_compensates_if_timeout_scheduling_fails(
        self, db_session, redis_client, caplog
    ):
        """Verify Saga compensation: If scheduling Redis ZSet timeout fails after DB commit,
        the order must be cancelled in PostgreSQL and the Redis seat lock released."""
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "7A")

        seat_key = f"flight:{flight.id}:seats"
        await redis_client.hset(seat_key, "7A", SeatState.AVAILABLE)

        with patch("services.schedule_order_timeout", new_callable=AsyncMock) as mock_sched:
            mock_sched.side_effect = RuntimeError("Redis ZSet timeout queue connection failed")

            with pytest.raises(RuntimeError) as exc_info:
                await reserve_seat(db_session, redis_client, user.id, flight.id, "7A")

            assert "Reservation cancelled" in str(exc_info.value)

        # 1. Redis seat lock MUST be released back to AVAILABLE ('0')
        seat_status = await redis_client.hget(seat_key, "7A")
        assert seat_status == SeatState.AVAILABLE

        # 2. Database order must NOT be left in PENDING status; it must be CANCELLED
        stmt = select(Order).where(Order.flight_id == flight.id, Order.seat_code == "7A")
        order_in_db = (await db_session.execute(stmt)).scalar_one_or_none()
        assert order_in_db is not None
        assert order_in_db.status == OrderStatus.CANCELLED

        # 3. Verify logging accuracy: timeout queue failure was logged, NOT a misleading DB commit failure
        assert "Failed to schedule timeout queue" in caplog.text
        assert "DB commit failed" not in caplog.text


class TestRedisZSetTimeoutQueue:
    async def test_schedule_and_process_expired_orders(self, db_session, redis_client):
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        seat = await _create_test_seat(db_session, flight.id, "7D")

        order = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="7D",
            status=OrderStatus.PENDING,
        )
        db_session.add(order)
        await db_session.commit()

        # Lock seat in Redis
        seat_key = f"flight:{flight.id}:seats"
        await redis_client.hset(seat_key, "7D", SeatState.LOCKED)

        # Schedule timeout with delay=-1 so score < now (immediately due)
        await schedule_order_timeout(redis_client, str(order.id), str(flight.id), "7D", delay=-1)

        # Verify entry exists in ZSet
        zcount = await redis_client.zcard(ORDER_TIMEOUT_QUEUE_KEY)
        assert zcount >= 1

        # Process due orders using Redis Lua script and cancel_expired_order worker
        processed_count = await process_expired_orders(redis_client, db=db_session)
        assert processed_count >= 1

        # Redis seat lock must be released back to '0'
        seat_state = await redis_client.hget(seat_key, "7D")
        assert seat_state == SeatState.AVAILABLE

        # Database order status must be cancelled
        res = await db_session.execute(select(Order).where(Order.id == order.id))
        refreshed_order = res.scalar_one_or_none()
        assert refreshed_order is not None
        assert refreshed_order.status == OrderStatus.CANCELLED

    async def test_process_expired_orders_requeues_with_delay_on_db_failure(self, redis_client):
        """Fault injection test: If cancel_expired_order throws DB/network error,
        the task must be re-added to Redis ZSet with a delay rather than being lost."""
        order_id = str(uuid.uuid4())
        flight_id = str(uuid.uuid4())
        seat_code = "8F"

        await schedule_order_timeout(redis_client, order_id, flight_id, seat_code, delay=-1)

        raw_task = json.dumps({
            "order_id": order_id,
            "flight_id": flight_id,
            "seat_code": seat_code,
        })

        # Inject simulated database connection crash
        with patch("services.cancel_expired_order", new_callable=AsyncMock) as mock_cancel:
            mock_cancel.side_effect = RuntimeError("Database connection timed out")
            before_process = datetime.now(timezone.utc).timestamp()
            processed_count = await process_expired_orders(redis_client)

            # Processing failed, so returned count is 0
            assert processed_count == 0

        # CRITICAL ASSERTION: Task was NOT lost upon pop! It must be re-queued in ZSet
        new_score = await redis_client.zscore(ORDER_TIMEOUT_QUEUE_KEY, raw_task)
        assert new_score is not None
        # Must be delayed by ~5 seconds into the future
        assert new_score >= before_process + 4.0

        # Retry counter in Redis must be incremented
        retries = await redis_client.get(f"order:timeout:retry:{order_id}")
        assert str(retries) == "1"

        # Cleanup
        await redis_client.zrem(ORDER_TIMEOUT_QUEUE_KEY, raw_task)
        await redis_client.delete(f"order:timeout:retry:{order_id}")

    async def test_process_expired_orders_drops_after_max_retries(self, redis_client):
        """Poison pill prevention: If a task repeatedly fails beyond max retries,
        it should be dropped from the queue to prevent infinite retry loops."""
        order_id = str(uuid.uuid4())
        flight_id = str(uuid.uuid4())
        seat_code = "9C"

        await schedule_order_timeout(redis_client, order_id, flight_id, seat_code, delay=-1)

        raw_task = json.dumps({
            "order_id": order_id,
            "flight_id": flight_id,
            "seat_code": seat_code,
        })

        # Pre-set retry counter to 3 (max retries reached)
        await redis_client.set(f"order:timeout:retry:{order_id}", "3")

        with patch("services.cancel_expired_order", new_callable=AsyncMock) as mock_cancel:
            mock_cancel.side_effect = RuntimeError("Permanent database corruption")
            processed_count = await process_expired_orders(redis_client)
            assert processed_count == 0

        # Task must NOT be re-queued anymore
        new_score = await redis_client.zscore(ORDER_TIMEOUT_QUEUE_KEY, raw_task)
        assert new_score is None

        # Cleanup
        await redis_client.delete(f"order:timeout:retry:{order_id}")


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

        # Initialize seat in Redis as available
        seat_key = f"flight:{flight.id}:seats"
        await redis_client.hset(seat_key, "14B", SeatState.AVAILABLE)

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

        # Also confirm seat is LOCKED in Redis
        seat_key = f"flight:{flight.id}:seats"
        assert await redis_client.hget(seat_key, "14B") == SeatState.LOCKED

    async def test_pay_order_confirms_seat_locked_in_redis(self, db_session, redis_client):
        """Verify pay_order explicitly confirms seat state as LOCKED in Redis."""
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "15C", status=0)

        order = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="15C",
            status=OrderStatus.PENDING,
        )
        db_session.add(order)
        await db_session.commit()

        # Seat key in Redis starts as available
        seat_key = f"flight:{flight.id}:seats"
        await redis_client.hset(seat_key, "15C", SeatState.AVAILABLE)

        await pay_order(db_session, order.id, user.id, redis=redis_client)

        # After payment, Redis seat must be confirmed as LOCKED ('1')
        seat_status = await redis_client.hget(seat_key, "15C")
        assert seat_status == SeatState.LOCKED


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

    async def test_pay_endpoint_already_processed_returns_409(self, async_client, db_session):
        import jwt
        from database import settings

        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "8B", status=0)

        order = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="8B",
            status=OrderStatus.CANCELLED,
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
        assert resp.status_code == 409
        assert "already processed or expired" in resp.json()["detail"]


class TestPayAndCancelConcurrency:
    async def test_worker_skips_seat_release_if_order_already_paid(self, db_session, redis_client):
        """Verify atomic CAS: If user paid, timeout worker must not release Redis seat."""
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        seat = await _create_test_seat(db_session, flight.id, "10A", status=0)
        order = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="10A",
            status=OrderStatus.PENDING,
        )
        db_session.add(order)
        await db_session.commit()

        seat_key = f"flight:{flight.id}:seats"
        await redis_client.hset(seat_key, "10A", SeatState.LOCKED)

        # 1. User pays the order -> status becomes CONFIRMED
        await pay_order(db_session, order.id, user.id)

        # 2. Worker attempts to cancel -> CAS fails because status is not PENDING
        cancelled = await cancel_expired_order(
            redis_client, str(order.id), str(flight.id), "10A", db=db_session
        )
        assert cancelled is False

        # 3. Redis seat must REMAIN LOCKED, NOT released to AVAILABLE ('0')
        redis_seat_status = await redis_client.hget(seat_key, "10A")
        assert redis_seat_status == SeatState.LOCKED

    async def test_pay_fails_if_worker_already_cancelled_order(self, db_session, redis_client):
        """Verify atomic CAS: If timeout worker cancelled order, payment attempt must fail."""
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        seat = await _create_test_seat(db_session, flight.id, "11B", status=0)
        order = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="11B",
            status=OrderStatus.PENDING,
        )
        db_session.add(order)
        await db_session.commit()

        seat_key = f"flight:{flight.id}:seats"
        await redis_client.hset(seat_key, "11B", SeatState.LOCKED)

        # 1. Worker auto-cancels the order -> status becomes CANCELLED
        cancelled = await cancel_expired_order(
            redis_client, str(order.id), str(flight.id), "11B", db=db_session
        )
        assert cancelled is True

        # 2. Redis seat must be released to AVAILABLE ('0')
        redis_seat_status = await redis_client.hget(seat_key, "11B")
        assert redis_seat_status == SeatState.AVAILABLE

        # 3. User attempts to pay afterwards -> atomic CAS fails and raises exception
        with pytest.raises(InvalidOrderStatusException):
            await pay_order(db_session, order.id, user.id)


class TestCancelOrderByUser:
    async def test_cancel_order_success_releases_seat_and_purges_zset(self, db_session, redis_client):
        user = await _create_test_user(db_session)
        user2 = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "12A", status=0)

        seat_key = f"flight:{flight.id}:seats"
        await redis_client.hset(seat_key, "12A", SeatState.LOCKED)

        order = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="12A",
            status=OrderStatus.PENDING,
        )
        db_session.add(order)
        await db_session.commit()

        await schedule_order_timeout(redis_client, str(order.id), str(flight.id), "12A")

        cancelled = await cancel_order_by_user(db_session, order.id, user.id, redis=redis_client)
        assert cancelled.status == OrderStatus.CANCELLED

        redis_status = await redis_client.hget(seat_key, "12A")
        assert redis_status == SeatState.AVAILABLE

        task_payload = json.dumps({
            "order_id": str(order.id),
            "flight_id": str(flight.id),
            "seat_code": "12A",
        })
        assert await redis_client.zscore(ORDER_TIMEOUT_QUEUE_KEY, task_payload) is None

        # User 2 can now reserve the released seat immediately
        order2 = await reserve_seat(db_session, redis_client, user2.id, flight.id, "12A")
        assert order2.seat_code == "12A"
        assert order2.status == OrderStatus.PENDING

    async def test_cancel_order_not_found_raises(self, db_session):
        user = await _create_test_user(db_session)
        with pytest.raises(OrderNotFoundException):
            await cancel_order_by_user(db_session, uuid.uuid4(), user.id)

    async def test_cancel_order_forbidden_for_other_user(self, db_session):
        user1 = await _create_test_user(db_session)
        user2 = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "12B", status=0)

        order = Order(
            id=uuid.uuid4(),
            user_id=user1.id,
            flight_id=flight.id,
            seat_code="12B",
            status=OrderStatus.PENDING,
        )
        db_session.add(order)
        await db_session.commit()

        with pytest.raises(OrderForbiddenException):
            await cancel_order_by_user(db_session, order.id, user2.id)

    async def test_cancel_order_already_confirmed_raises(self, db_session):
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "12C", status=1)

        order = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="12C",
            status=OrderStatus.CONFIRMED,
        )
        db_session.add(order)
        await db_session.commit()

        with pytest.raises(InvalidOrderStatusException):
            await cancel_order_by_user(db_session, order.id, user.id)

    async def test_cancel_order_already_cancelled_raises(self, db_session):
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "12D", status=0)

        order = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="12D",
            status=OrderStatus.CANCELLED,
        )
        db_session.add(order)
        await db_session.commit()

        with pytest.raises(InvalidOrderStatusException):
            await cancel_order_by_user(db_session, order.id, user.id)

    async def test_cancel_order_by_user_retries_redis_seat_release(self, db_session, redis_client):
        """Verify cancel_order_by_user retries releasing Redis seat if initial attempt fails."""
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "12E", status=0)

        order = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="12E",
            status=OrderStatus.PENDING,
        )
        db_session.add(order)
        await db_session.commit()

        seat_key = f"flight:{flight.id}:seats"
        await redis_client.hset(seat_key, "12E", SeatState.LOCKED)

        # First seat update call raises Redis connection error, second succeeds
        import services
        real_hset_if_exists = services._hset_seat_if_exists
        attempt_count = 0

        async def flaky_hset_if_exists(redis, flight_id, seat_code, state):
            nonlocal attempt_count
            if seat_code == "12E":
                attempt_count += 1
                if attempt_count == 1:
                    raise RuntimeError("Transient Redis connection drop")
            return await real_hset_if_exists(redis, flight_id, seat_code, state)

        with patch("services._hset_seat_if_exists", side_effect=flaky_hset_if_exists):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                cancelled = await cancel_order_by_user(db_session, order.id, user.id, redis=redis_client)
                assert cancelled.status == OrderStatus.CANCELLED

        # Verify retry took place and seat was successfully freed
        assert attempt_count == 2
        redis_status = await redis_client.hget(seat_key, "12E")
        assert redis_status == SeatState.AVAILABLE


class TestCancelOrderAPI:
    async def test_api_cancel_order_endpoint_success(self, async_client, db_session, redis_client):
        import jwt
        from database import settings

        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "13A", status=0)

        seat_key = f"flight:{flight.id}:seats"
        await redis_client.hset(seat_key, "13A", SeatState.LOCKED)

        order = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="13A",
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

        resp = await async_client.post(f"/api/v1/orders/{order.id}/cancel", headers=headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "Cancelled"
        assert data["id"] == str(order.id)

        redis_status = await redis_client.hget(seat_key, "13A")
        assert redis_status == SeatState.AVAILABLE

    async def test_api_cancel_order_forbidden(self, async_client, db_session):
        import jwt
        from database import settings

        user1 = await _create_test_user(db_session)
        user2 = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "13B", status=0)

        order = Order(
            id=uuid.uuid4(),
            user_id=user1.id,
            flight_id=flight.id,
            seat_code="13B",
            status=OrderStatus.PENDING,
        )
        db_session.add(order)
        await db_session.commit()

        token2 = jwt.encode(
            {"sub": str(user2.id), "type": "access"},
            settings.JWT_SECRET_KEY,
            algorithm=settings.JWT_ALGORITHM,
        )
        headers = {"Authorization": f"Bearer {token2}"}

        resp = await async_client.post(f"/api/v1/orders/{order.id}/cancel", headers=headers)
        assert resp.status_code == 403


class TestGlobalExceptionHandler:
    async def test_redis_timeout_error_returns_503(self, async_client):
        """Verify Redis TimeoutError is handled by the global RedisError handler returning 503 instead of 500."""
        from redis.exceptions import TimeoutError as RedisTimeoutError
        with patch("api.get_flight_seat_map", side_effect=RedisTimeoutError("Redis socket read timed out")):
            fake_id = uuid.uuid4()
            resp = await async_client.get(f"/api/v1/flights/{fake_id}/seats")
            assert resp.status_code == 503
            assert "Service temporarily unavailable" in resp.json()["detail"]


class TestSettingsSecurity:
    async def test_settings_fails_fast_without_jwt_secret(self, monkeypatch):
        """Verify fail-fast security baseline: Missing JWT_SECRET_KEY prevents application bootstrap."""
        from pydantic import ValidationError
        from database import Settings

        monkeypatch.delenv("JWT_SECRET_KEY", raising=False)
        with pytest.raises(ValidationError) as exc_info:
            Settings(_env_file=None)
        assert "JWT_SECRET_KEY" in str(exc_info.value)


class TestSeatMapPartialHashDefense:
    async def test_release_seat_when_key_missing_does_not_corrupt_cache_and_hydrates_all_seats(
        self, db_session, redis_client
    ):
        """Verify Bug-01 fix: Releasing a seat on an evicted/missing Redis key must not create
        a 1-seat partial Hash, ensuring subsequent seat map queries hydrate all seats from DB.
        """
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "1A", status=0)
        await _create_test_seat(db_session, flight.id, "1B", status=0)
        await _create_test_seat(db_session, flight.id, "1C", status=0)

        seat_key = f"flight:{flight.id}:seats"
        await redis_client.delete(seat_key)
        assert await redis_client.exists(seat_key) == 0

        # Attempt to release seat 1A while key is missing
        fake_order_id = uuid.uuid4()
        released = await release_seat_with_retry(redis_client, flight.id, "1A", fake_order_id)
        assert released is True

        # Crucial assertion: Redis key must NOT have been created with only 1A
        assert await redis_client.exists(seat_key) == 0

        # Subsequent seat map query must fully hydrate all 3 seats from PostgreSQL
        seat_map = await get_flight_seat_map(db_session, redis_client, flight.id)
        assert seat_map.total_seats == 3
        assert seat_map.available_seats == 3
        assert [s.seat_code for s in seat_map.seats] == ["1A", "1B", "1C"]

    async def test_pay_order_when_key_missing_does_not_corrupt_cache_and_hydrates_correctly(
        self, db_session, redis_client
    ):
        """Verify Bug-01 fix: Confirming payment on a missing Redis key must not create
        a 1-seat partial Hash; full hydration must reflect the paid seat as locked.
        """
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "2A", status=0)
        await _create_test_seat(db_session, flight.id, "2B", status=0)
        await _create_test_seat(db_session, flight.id, "2C", status=0)

        order = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="2A",
            status=OrderStatus.PENDING,
        )
        db_session.add(order)
        await db_session.commit()

        seat_key = f"flight:{flight.id}:seats"
        await redis_client.delete(seat_key)
        assert await redis_client.exists(seat_key) == 0

        # Pay order when key is missing in Redis
        await pay_order(db_session, order.id, user.id, redis=redis_client)

        # Redis key must NOT have been created
        assert await redis_client.exists(seat_key) == 0

        # Subsequent seat map query hydrates from DB, showing 2A locked and others available
        seat_map = await get_flight_seat_map(db_session, redis_client, flight.id)
        assert seat_map.total_seats == 3
        assert seat_map.available_seats == 2
        seat_2a = next(s for s in seat_map.seats if s.seat_code == "2A")
        assert seat_2a.status == 1
        assert seat_2a.is_available is False

    async def test_timeout_cancellation_when_key_missing_does_not_corrupt_cache(
        self, db_session, redis_client
    ):
        """Verify Bug-01 fix: Worker auto-cancellation on missing Redis key preserves DB hydration."""
        user = await _create_test_user(db_session)
        flight = await _create_test_flight(db_session)
        await _create_test_seat(db_session, flight.id, "3A", status=0)
        await _create_test_seat(db_session, flight.id, "3B", status=0)

        order = Order(
            id=uuid.uuid4(),
            user_id=user.id,
            flight_id=flight.id,
            seat_code="3A",
            status=OrderStatus.PENDING,
        )
        db_session.add(order)
        await db_session.commit()

        seat_key = f"flight:{flight.id}:seats"
        await redis_client.delete(seat_key)
        assert await redis_client.exists(seat_key) == 0

        # Worker cancels expired order
        success = await cancel_expired_order(
            redis_client, str(order.id), str(flight.id), "3A", db=db_session
        )
        assert success is True

        # Key must not be created
        assert await redis_client.exists(seat_key) == 0

        # Both seats available on hydration
        seat_map = await get_flight_seat_map(db_session, redis_client, flight.id)
        assert seat_map.total_seats == 2
        assert seat_map.available_seats == 2
