import uuid
from datetime import date
from typing import Annotated

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db, get_redis, settings
from models import FlightResponse, FlightSeatMapResponse, OrderResponse, ReserveRequest
from auth import get_current_user_id
from rate_limiter import RateLimiter
from services import (
    cancel_order_by_user,
    get_flight_seat_map,
    pay_order,
    reserve_seat,
    search_flights,
)

router = APIRouter(prefix="/api/v1", tags=["Flights"])

reserve_limiter = RateLimiter(
    times=settings.RATE_LIMIT_RESERVE_TIMES,
    seconds=settings.RATE_LIMIT_RESERVE_SECONDS,
)


@router.get("/flights", response_model=list[FlightResponse])
async def list_flights(
    response: Response,
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
    departure: Annotated[str, Query(min_length=1, max_length=64)],
    arrival: Annotated[str, Query(min_length=1, max_length=64)],
    travel_date: Annotated[date, Query(description="Travel date in YYYY-MM-DD format")],
) -> list[FlightResponse]:
    flights, is_from_cache = await search_flights(
        db, redis, departure.strip().upper(), arrival.strip().upper(), travel_date,
    )

    response.headers["X-Cache-Status"] = "HIT" if is_from_cache else "MISS"

    return flights


@router.get("/flights/{flight_id}/seats", response_model=FlightSeatMapResponse)
async def get_flight_seats(
    flight_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
) -> FlightSeatMapResponse:
    return await get_flight_seat_map(db, redis, flight_id)


@router.post(
    "/orders/reserve",
    response_model=OrderResponse,
    status_code=201,
    dependencies=[Depends(reserve_limiter)],
)
async def reserve_flight_seat(
    req: ReserveRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
    user_id: Annotated[uuid.UUID, Depends(get_current_user_id)],
) -> OrderResponse:
    order = await reserve_seat(
        db, redis, user_id, req.flight_id, req.seat_code
    )
    return OrderResponse.model_validate(order)


@router.post("/orders/{order_id}/pay", response_model=OrderResponse)
async def pay_flight_order(
    order_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
    user_id: Annotated[uuid.UUID, Depends(get_current_user_id)],
) -> OrderResponse:
    order = await pay_order(db, order_id, user_id, redis=redis)
    return OrderResponse.model_validate(order)


@router.post("/orders/{order_id}/cancel", response_model=OrderResponse)
async def cancel_flight_order(
    order_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[aioredis.Redis, Depends(get_redis)],
    user_id: Annotated[uuid.UUID, Depends(get_current_user_id)],
) -> OrderResponse:
    order = await cancel_order_by_user(db, order_id, user_id, redis=redis)
    return OrderResponse.model_validate(order)
