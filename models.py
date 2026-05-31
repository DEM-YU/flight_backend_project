import uuid
from datetime import datetime, timezone
from enum import StrEnum

from pydantic import BaseModel, ConfigDict
from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


# ── Domain Constants ──

class OrderStatus(StrEnum):
    """Finite state machine states for order lifecycle."""
    PENDING = "Pending"
    CONFIRMED = "Confirmed"
    CANCELLED = "Cancelled"


class SeatState(StrEnum):
    """Redis seat hash value semantics."""
    AVAILABLE = "0"
    LOCKED = "1"

# ORM Base
class Base(DeclarativeBase):
    pass

# ORM Models
class Flight(Base):
    # Flight Table: Leverages compound index to optimize high-frequency route queries.
    __tablename__ = "flights"
    __table_args__ = (
        Index("ix_flight_departure_arrival", "departure", "arrival"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    flight_num: Mapped[str] = mapped_column(String(16), nullable=False)
    departure: Mapped[str] = mapped_column(String(64), nullable=False)
    arrival: Mapped[str] = mapped_column(String(64), nullable=False)
    departure_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Seat(Base):
    # Seat Table: Tracks localized seat locking states per flight for isolation.
    __tablename__ = "seats"
    __table_args__ = (
        UniqueConstraint("flight_id", "seat_code", name="uq_seat_flight_seat_code"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    flight_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("flights.id", ondelete="CASCADE"), nullable=False
    )
    seat_code: Mapped[str] = mapped_column(String(8), nullable=False)
    # 0 = available, 1 = locked
    status: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

class Order(Base):
    # Order Table: Records user booking behaviors with a default 'Pending' state.
    __tablename__ = "orders"
    __table_args__ = (
        Index("ix_order_status_created", "status", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    flight_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("flights.id", ondelete="RESTRICT"), nullable=False
    )
    seat_code: Mapped[str] = mapped_column(String(8), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), default=OrderStatus.PENDING, nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

# pydantic schemas

class FlightResponse(BaseModel):
    # Flight information response schema.
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    flight_num: str
    departure: str
    arrival: str
    departure_time: datetime


class ReserveRequest(BaseModel):
   # Seat pre-locking request schema.
    flight_id: uuid.UUID
    seat_code: str


class OrderResponse(BaseModel):
    # Order response schema.
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    user_id: uuid.UUID
    flight_id: uuid.UUID
    seat_code: str
    status: str
    created_at: datetime
