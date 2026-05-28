import uuid
from typing import Annotated

import redis.asyncio as aioredis
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Response
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db, get_redis
from models import FlightResponse, OrderResponse, ReserveRequest
from services import SeatUnavailableException, process_order_timeout, reserve_seat, search_flights

router = APIRouter(prefix="/api/v1", tags=["Flights"])


@router.get("/flights", response_model=list[FlightResponse])
async def list_flights(
    departure: str,
    arrival: str,
    date: str,  # YYYY-MM-DD
    response: Response,
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
) -> list[FlightResponse]:

    # Flight route query API strictly executing the Cache-Aside pattern.
    # Response header 'X-Cache-Status' is included for monitoring and debugging.

    flights, is_from_cache = await search_flights(db, redis, departure, arrival, date)

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
    - Returns HTTP 409 on seat conflicts.
    """
    # Mock current authenticated user (in production injected by JWT middleware)
    mock_user_id = uuid.uuid4()
    try:
        order = await reserve_seat(
            db, redis, mock_user_id, payload.flight_id, payload.seat_code
        )
    except SeatUnavailableException:
        raise HTTPException(
            status_code=409,
            detail="Seat is already reserved or unavailable.",
        )