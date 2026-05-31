import uuid
from datetime import date
from typing import Annotated

import redis.asyncio as aioredis
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Response
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db, get_redis
from models import FlightResponse, OrderResponse, ReserveRequest
from services import (
    SeatNotFoundException,
    SeatUnavailableException,
    process_order_timeout,
    reserve_seat,
    search_flights,
)

router = APIRouter(prefix="/api/v1", tags=["Flights"])


@router.get("/flights", response_model=list[FlightResponse])
async def list_flights(
    response: Response,
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
    departure: Annotated[str, Query(min_length=1, max_length=64)],
    arrival: Annotated[str, Query(min_length=1, max_length=64)],
    travel_date: Annotated[date, Query(description="Travel date in YYYY-MM-DD format")],
) -> list[FlightResponse]:

    # Flight route query API strictly executing the Cache-Aside pattern.
    # Response header 'X-Cache-Status' is included for monitoring and debugging.

    flights, is_from_cache = await search_flights(
        db, redis, departure.strip().upper(), arrival.strip().upper(), travel_date,
    )

    # Inject cache status response header
    response.headers["X-Cache-Status"] = "HIT" if is_from_cache else "MISS"

    return flights


@router.post("/orders/reserve", response_model=OrderResponse, status_code=201)
async def reserve_flight_seat(
    payload: ReserveRequest,
    background_tasks: BackgroundTasks,
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
) -> OrderResponse:
    """
    Atomic seat reservation API.
    - Uses Redis Lua scripts to ensure concurrency safety and prevent overselling.
    - Persists a 'Pending' order in PostgreSQL immediately upon successful lock.
    - Automatically registers a 60-second timeout release task for eventual consistency.
    - Returns HTTP 404 if seat does not exist.
    - Returns HTTP 409 on seat conflicts.
    """
    # Mock current authenticated user (in production injected by JWT middleware)
    mock_user_id = uuid.uuid4()
    try:
        order = await reserve_seat(
            db, redis, mock_user_id, payload.flight_id, payload.seat_code
        )
    except SeatNotFoundException:
        raise HTTPException(
            status_code=404,
            detail="Seat does not exist or flight seat map is not initialized.",
        )
    except SeatUnavailableException:
        raise HTTPException(
            status_code=409,
            detail="Seat is already reserved or unavailable.",
        )

    # Register distributed timeout release task asynchronously
    background_tasks.add_task(
        process_order_timeout,
        redis,
        str(order.id),
        str(payload.flight_id),
        payload.seat_code,
    )

    return OrderResponse.model_validate(order)