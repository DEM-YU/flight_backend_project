
import json
import uuid
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from models import FlightResponse, OrderStatus, SeatState
from services import (
    SeatNotFoundException,
    SeatUnavailableException,
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
