"""
Unit tests for services.py

These tests mock both Redis and PostgreSQL so they run without any real
infrastructure. The goal is to verify the core reservation logic in isolation:
  - Cache-Aside pattern for flight search
  - Atomic Lua seat locking and its three outcomes
  - Redis compensating rollback when the DB write fails
  - Pending-order auto-cancellation and seat release
"""

import json
import uuid
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from models import FlightResponse, OrderStatus, SeatState
from services import (
    ORDER_TIMEOUT_QUEUE_KEY,
    InvalidOrderStatusException,
    OrderForbiddenException,
    OrderNotFoundException,
    SeatNotFoundException,
    SeatUnavailableException,
    cancel_expired_order,
    pay_order,
    process_expired_orders,
    process_order_timeout,
    reserve_seat,
    schedule_order_timeout,
    search_flights,
)

# pytest-asyncio configuration
pytestmark = pytest.mark.asyncio


# Helper: build a minimal FlightResponse dict for cache payloads
def _flight_dict(**overrides) -> dict:
    defaults = {
        "id": str(uuid.uuid4()),
        "flight_num": "CA001",
        "departure": "PEK",
        "arrival": "SHA",
        "departure_time": datetime.now(timezone.utc).isoformat(),
    }
    return {**defaults, **overrides}


# search_flights — Cache-Aside behaviour

class TestSearchFlights:
    """Tests for the Cache-Aside pattern in search_flights()."""

    async def test_cache_hit_returns_cached_data_without_db_query(self):
        """When Redis has a cached result, PostgreSQL must NOT be queried."""
        db = AsyncMock()
        redis = AsyncMock()

        # Simulate a warm cache containing one flight
        cached = [_flight_dict(flight_num="CA001")]
        redis.get.return_value = json.dumps(cached)

        results, is_cache_hit = await search_flights(db, redis, "PEK", "SHA", date.today())

        assert is_cache_hit is True
        assert len(results) == 1
        assert results[0].flight_num == "CA001"
        db.execute.assert_not_called()  # DB must be bypassed on cache hit

    async def test_cache_miss_queries_db_and_writes_back(self):
        """On a cache miss, the DB is queried and the result is written to Redis."""
        db = AsyncMock()
        redis = AsyncMock()
        redis.get.return_value = None  # cold cache

        # Build a mock ORM object with the attributes FlightResponse needs
        mock_flight = MagicMock()
        mock_flight.id = uuid.uuid4()
        mock_flight.flight_num = "MU200"
        mock_flight.departure = "PEK"
        mock_flight.arrival = "SHA"
        mock_flight.departure_time = datetime.now(timezone.utc)

        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [mock_flight]
        db.execute.return_value = mock_result

        results, is_cache_hit = await search_flights(db, redis, "PEK", "SHA", date.today())

        assert is_cache_hit is False
        assert len(results) == 1
        assert results[0].flight_num == "MU200"
        redis.setex.assert_called_once()  # result must be written back to Redis

    async def test_empty_result_uses_short_ttl(self):
        """
        An empty DB result is still cached, but with a short TTL (60 s) so
        newly added flights become visible quickly.
        """
        db = AsyncMock()
        redis = AsyncMock()
        redis.get.return_value = None  # cold cache

        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []  # no flights found
        db.execute.return_value = mock_result

        results, _ = await search_flights(db, redis, "PEK", "CAN", date.today())

        assert results == []
        # setex(key, ttl, value) — verify ttl is the short one (60 s)
        _, ttl, _ = redis.setex.call_args.args
        assert ttl == 60


# reserve_seat — Lua lock outcomes

class TestReserveSeat:
    """Tests for the three Lua script outcomes and DB-failure compensation."""

    async def test_success_creates_pending_order(self):
        """Lua returns 1 (locked) → an Order with PENDING status is persisted."""
        db = MagicMock()
        db.add = MagicMock()
        db.commit = AsyncMock()
        db.rollback = AsyncMock()
        db.refresh = AsyncMock()
        redis = AsyncMock()
        redis.eval.return_value = 1  # seat successfully locked in Redis

        order = await reserve_seat(db, redis, uuid.uuid4(), uuid.uuid4(), "1A")

        db.add.assert_called_once()
        db.commit.assert_called_once()
        assert order.status == OrderStatus.PENDING

    async def test_seat_already_locked_raises_unavailable(self):
        """Lua returns 0 (already locked) → SeatUnavailableException, DB untouched."""
        db = MagicMock()
        db.add = MagicMock()
        redis = AsyncMock()
        redis.eval.return_value = 0  # seat taken by another request

        with pytest.raises(SeatUnavailableException):
            await reserve_seat(db, redis, uuid.uuid4(), uuid.uuid4(), "1A")

        db.add.assert_not_called()

    async def test_seat_not_initialized_raises_not_found(self):
        """Lua returns -1 (hash key missing) → SeatNotFoundException, DB untouched."""
        db = MagicMock()
        db.add = MagicMock()
        redis = AsyncMock()
        redis.eval.return_value = -1  # seat hash was never seeded

        with pytest.raises(SeatNotFoundException):
            await reserve_seat(db, redis, uuid.uuid4(), uuid.uuid4(), "99Z")

        db.add.assert_not_called()

    async def test_db_failure_releases_redis_lock(self):
        """
        If the DB commit fails after the Redis lock is acquired, the lock must
        be rolled back to AVAILABLE. Without this, the seat would be locked in
        Redis with no matching order in PostgreSQL ('ghost lock').
        """
        db = MagicMock()
        db.add = MagicMock()
        db.commit = AsyncMock(side_effect=Exception("connection lost"))
        db.rollback = AsyncMock()
        redis = AsyncMock()
        redis.eval.return_value = 1  # Redis lock acquired

        flight_id = uuid.uuid4()
        seat_code = "3B"

        with pytest.raises(Exception, match="connection lost"):
            await reserve_seat(db, redis, uuid.uuid4(), flight_id, seat_code)

        # Compensating write: Redis lock must be released
        redis.hset.assert_called_once_with(
            f"flight:{flight_id}:seats",
            seat_code,
            SeatState.AVAILABLE,
        )


# process_order_timeout — background auto-cancellation

class TestProcessOrderTimeout:
    """Tests for the 60-second payment-window auto-cancellation task."""

    async def test_pending_order_is_cancelled_and_seat_released(self):
        """
        After the timeout, a PENDING order is transitioned to CANCELLED and
        the Redis seat lock is released.
        """
        redis = AsyncMock()
        order_id = str(uuid.uuid4())
        flight_id = str(uuid.uuid4())
        seat_code = "5C"

        # Build a mock order whose status starts as PENDING
        mock_order = MagicMock()
        mock_order.status = OrderStatus.PENDING

        mock_db_result = MagicMock()
        mock_db_result.scalar_one_or_none.return_value = mock_order

        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_db_result
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("services.AsyncSessionLocal", return_value=mock_session):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                await process_order_timeout(redis, order_id, flight_id, seat_code, delay=0)

        assert mock_order.status == OrderStatus.CANCELLED
        mock_session.commit.assert_called_once()
        redis.hset.assert_called_once_with(
            f"flight:{flight_id}:seats", seat_code, SeatState.AVAILABLE
        )

    async def test_confirmed_order_is_not_touched(self):
        """
        If the order was already CONFIRMED (payment received), the timeout
        task must do nothing — no status change, no seat release.
        """
        redis = AsyncMock()
        order_id = str(uuid.uuid4())

        mock_order = MagicMock()
        mock_order.status = OrderStatus.CONFIRMED  # already paid

        mock_db_result = MagicMock()
        mock_db_result.scalar_one_or_none.return_value = mock_order

        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_db_result
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("services.AsyncSessionLocal", return_value=mock_session):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                await process_order_timeout(redis, order_id, "flight-id", "1A", delay=0)

        mock_session.commit.assert_not_called()
        redis.hset.assert_not_called()

    async def test_missing_order_is_handled_gracefully(self):
        """
        If the order record is not found in DB (e.g. rolled back earlier),
        the task exits cleanly without raising an exception.
        """
        redis = AsyncMock()

        mock_db_result = MagicMock()
        mock_db_result.scalar_one_or_none.return_value = None  # order gone

        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_db_result
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("services.AsyncSessionLocal", return_value=mock_session):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                # Should not raise
                await process_order_timeout(redis, str(uuid.uuid4()), "f", "1A", delay=0)

        redis.hset.assert_not_called()


class TestRedisZSetTimeoutQueue:
    """Tests for crash-resilient Redis ZSet timeout queue."""

    async def test_schedule_order_timeout(self):
        """schedule_order_timeout adds order payload to Redis ZSet with score."""
        redis = AsyncMock()
        order_id = str(uuid.uuid4())
        flight_id = str(uuid.uuid4())
        seat_code = "7D"

        await schedule_order_timeout(redis, order_id, flight_id, seat_code, delay=60)

        redis.zadd.assert_called_once()
        key, mapping = redis.zadd.call_args.args
        assert key == ORDER_TIMEOUT_QUEUE_KEY
        payload_str = list(mapping.keys())[0]
        payload = json.loads(payload_str)
        assert payload["order_id"] == order_id
        assert payload["flight_id"] == flight_id
        assert payload["seat_code"] == seat_code

    async def test_process_expired_orders(self):
        """process_expired_orders pops due orders from ZSet and triggers cancellation."""
        redis = AsyncMock()
        order_id = str(uuid.uuid4())
        flight_id = str(uuid.uuid4())
        seat_code = "8E"
        raw_payload = json.dumps({"order_id": order_id, "flight_id": flight_id, "seat_code": seat_code})

        # Lua atomic pop returns 1 expired item
        redis.eval.return_value = [raw_payload]

        with patch("services.cancel_expired_order", new_callable=AsyncMock) as mock_cancel:
            count = await process_expired_orders(redis)
            assert count == 1
            mock_cancel.assert_called_once_with(redis, order_id, flight_id, seat_code)

    async def test_process_expired_orders_empty(self):
        """When no orders are expired, process_expired_orders returns 0."""
        redis = AsyncMock()
        redis.eval.return_value = []

        count = await process_expired_orders(redis)
        assert count == 0


# pay_order — order payment confirmation and seat status persistence

class TestPayOrder:
    """Tests for pay_order() confirming payment and updating seat status."""

    async def test_pay_order_success(self):
        """A pending order owned by user is confirmed and seat is marked occupied (1)."""
        db = AsyncMock()
        order_id = uuid.uuid4()
        user_id = uuid.uuid4()
        flight_id = uuid.uuid4()
        seat_code = "12A"

        mock_order = MagicMock()
        mock_order.id = order_id
        mock_order.user_id = user_id
        mock_order.flight_id = flight_id
        mock_order.seat_code = seat_code
        mock_order.status = OrderStatus.PENDING

        mock_seat = MagicMock()
        mock_seat.status = 0

        # db.execute calls: first for Order, second for Seat
        order_exec_result = MagicMock()
        order_exec_result.scalar_one_or_none.return_value = mock_order

        seat_exec_result = MagicMock()
        seat_exec_result.scalar_one_or_none.return_value = mock_seat

        db.execute.side_effect = [order_exec_result, seat_exec_result]

        paid_order = await pay_order(db, order_id, user_id)

        assert paid_order.status == OrderStatus.CONFIRMED
        assert mock_seat.status == 1
        db.commit.assert_called_once()
        db.refresh.assert_called_once_with(mock_order)

    async def test_pay_order_not_found_raises(self):
        """Paying a non-existent order raises OrderNotFoundException."""
        db = AsyncMock()
        order_exec_result = MagicMock()
        order_exec_result.scalar_one_or_none.return_value = None
        db.execute.return_value = order_exec_result

        with pytest.raises(OrderNotFoundException):
            await pay_order(db, uuid.uuid4(), uuid.uuid4())

    async def test_pay_order_forbidden_for_other_user(self):
        """A user cannot pay for someone else's order."""
        db = AsyncMock()
        owner_id = uuid.uuid4()
        different_user_id = uuid.uuid4()

        mock_order = MagicMock()
        mock_order.user_id = owner_id
        order_exec_result = MagicMock()
        order_exec_result.scalar_one_or_none.return_value = mock_order
        db.execute.return_value = order_exec_result

        with pytest.raises(OrderForbiddenException):
            await pay_order(db, uuid.uuid4(), different_user_id)

    async def test_pay_order_already_cancelled_raises(self):
        """Paying an already CANCELLED order raises InvalidOrderStatusException."""
        db = AsyncMock()
        user_id = uuid.uuid4()

        mock_order = MagicMock()
        mock_order.user_id = user_id
        mock_order.status = OrderStatus.CANCELLED
        order_exec_result = MagicMock()
        order_exec_result.scalar_one_or_none.return_value = mock_order
        db.execute.return_value = order_exec_result

        with pytest.raises(InvalidOrderStatusException):
            await pay_order(db, uuid.uuid4(), user_id)

    async def test_pay_order_already_confirmed_raises(self):
        """Paying an already CONFIRMED order raises InvalidOrderStatusException."""
        db = AsyncMock()
        user_id = uuid.uuid4()

        mock_order = MagicMock()
        mock_order.user_id = user_id
        mock_order.status = OrderStatus.CONFIRMED
        order_exec_result = MagicMock()
        order_exec_result.scalar_one_or_none.return_value = mock_order
        db.execute.return_value = order_exec_result

        with pytest.raises(InvalidOrderStatusException):
            await pay_order(db, uuid.uuid4(), user_id)


class TestPayOrderAPI:
    """API integration tests for POST /api/v1/orders/{order_id}/pay."""

    async def test_pay_endpoint_success(self):
        from httpx import AsyncClient, ASGITransport
        from main import app
        from database import get_db
        from auth import get_current_user_id

        user_id = uuid.uuid4()
        order_id = uuid.uuid4()

        mock_db = MagicMock()
        mock_order = MagicMock()
        mock_order.id = order_id
        mock_order.user_id = user_id
        mock_order.flight_id = uuid.uuid4()
        mock_order.seat_code = "1A"
        mock_order.status = OrderStatus.PENDING
        mock_order.created_at = datetime.now(timezone.utc)

        mock_seat = MagicMock()
        mock_seat.status = 0

        order_res = MagicMock()
        order_res.scalar_one_or_none.return_value = mock_order
        seat_res = MagicMock()
        seat_res.scalar_one_or_none.return_value = mock_seat

        mock_db.execute = AsyncMock(side_effect=[order_res, seat_res])
        mock_db.commit = AsyncMock()
        mock_db.refresh = AsyncMock()

        app.dependency_overrides[get_db] = lambda: mock_db
        app.dependency_overrides[get_current_user_id] = lambda: user_id
        try:
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
                resp = await client.post(f"/api/v1/orders/{order_id}/pay")
                assert resp.status_code == 200
                data = resp.json()
                assert data["status"] == "Confirmed"
                assert data["id"] == str(order_id)
        finally:
            app.dependency_overrides.clear()
