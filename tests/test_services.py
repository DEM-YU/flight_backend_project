
import json
import uuid
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from models import FlightResponse, OrderStatus, SeatState
from services import (
    InvalidOrderStatusException,
    OrderForbiddenException,
    OrderNotFoundException,
    SeatNotFoundException,
    SeatUnavailableException,
    pay_order,
    process_order_timeout,
    reserve_seat,
    search_flights,
)


pytestmark = pytest.mark.asyncio



def _flight_dict(**overrides) -> dict:
    defaults = {
        "id": str(uuid.uuid4()),
        "flight_num": "CA001",
        "departure": "PEK",
        "arrival": "SHA",
        "departure_time": datetime.now(timezone.utc).isoformat(),
    }
    return {**defaults, **overrides}



class TestSearchFlights:

    async def test_cache_hit_returns_cached_data_without_db_query(self):

        db = AsyncMock()
        redis = AsyncMock()


        cached = [_flight_dict(flight_num="CA001")]
        redis.get.return_value = json.dumps(cached)

        results, is_cache_hit = await search_flights(db, redis, "PEK", "SHA", date.today())

        assert is_cache_hit is True
        assert len(results) == 1
        assert results[0].flight_num == "CA001"
        db.execute.assert_not_called()

    async def test_cache_miss_queries_db_and_writes_back(self):

        db = AsyncMock()
        redis = AsyncMock()
        redis.get.return_value = None


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
        redis.setex.assert_called_once()

    async def test_empty_result_uses_short_ttl(self):

        db = AsyncMock()
        redis = AsyncMock()
        redis.get.return_value = None

        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        db.execute.return_value = mock_result

        results, _ = await search_flights(db, redis, "PEK", "CAN", date.today())

        assert results == []

        _, ttl, _ = redis.setex.call_args.args
        assert ttl == 60



class TestReserveSeat:

    async def test_success_creates_pending_order(self):

        db = MagicMock()
        db.add = MagicMock()
        db.commit = AsyncMock()
        db.rollback = AsyncMock()
        db.refresh = AsyncMock()
        redis = AsyncMock()
        redis.eval.return_value = 1

        order = await reserve_seat(db, redis, uuid.uuid4(), uuid.uuid4(), "1A")

        db.add.assert_called_once()
        db.commit.assert_called_once()
        assert order.status == OrderStatus.PENDING

    async def test_seat_already_locked_raises_unavailable(self):

        db = MagicMock()
        db.add = MagicMock()
        redis = AsyncMock()
        redis.eval.return_value = 0

        with pytest.raises(SeatUnavailableException):
            await reserve_seat(db, redis, uuid.uuid4(), uuid.uuid4(), "1A")

        db.add.assert_not_called()

    async def test_seat_not_initialized_raises_not_found(self):

        db = MagicMock()
        db.add = MagicMock()
        redis = AsyncMock()
        redis.eval.return_value = -1

        with pytest.raises(SeatNotFoundException):
            await reserve_seat(db, redis, uuid.uuid4(), uuid.uuid4(), "99Z")

        db.add.assert_not_called()

    async def test_db_failure_releases_redis_lock(self):

        db = MagicMock()
        db.add = MagicMock()
        db.commit = AsyncMock(side_effect=Exception("connection lost"))
        db.rollback = AsyncMock()
        redis = AsyncMock()
        redis.eval.return_value = 1

        flight_id = uuid.uuid4()
        seat_code = "3B"

        with pytest.raises(Exception, match="connection lost"):
            await reserve_seat(db, redis, uuid.uuid4(), flight_id, seat_code)


        redis.hset.assert_called_once_with(
            f"flight:{flight_id}:seats",
            seat_code,
            SeatState.AVAILABLE,
        )



class TestProcessOrderTimeout:

    async def test_pending_order_is_cancelled_and_seat_released(self):

        redis = AsyncMock()
        order_id = str(uuid.uuid4())
        flight_id = str(uuid.uuid4())
        seat_code = "5C"


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

        redis = AsyncMock()
        order_id = str(uuid.uuid4())

        mock_order = MagicMock()
        mock_order.status = OrderStatus.CONFIRMED

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

        redis = AsyncMock()

        mock_db_result = MagicMock()
        mock_db_result.scalar_one_or_none.return_value = None

        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_db_result
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("services.AsyncSessionLocal", return_value=mock_session):
            with patch("asyncio.sleep", new_callable=AsyncMock):

                await process_order_timeout(redis, str(uuid.uuid4()), "f", "1A", delay=0)

        redis.hset.assert_not_called()



class TestPayOrder:

    async def test_pay_order_success(self):

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

        db = AsyncMock()
        order_exec_result = MagicMock()
        order_exec_result.scalar_one_or_none.return_value = None
        db.execute.return_value = order_exec_result

        with pytest.raises(OrderNotFoundException):
            await pay_order(db, uuid.uuid4(), uuid.uuid4())

    async def test_pay_order_forbidden_for_other_user(self):

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
