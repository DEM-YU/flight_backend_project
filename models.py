import uuid
from datetime import datetime, timezone
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, EmailStr, Field
from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column




class OrderStatus(StrEnum):
    PENDING = "Pending"
    CONFIRMED = "Confirmed"
    CANCELLED = "Cancelled"


class SeatState(StrEnum):
    AVAILABLE = "0"
    LOCKED = "1"


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    email: Mapped[str] = mapped_column(
        String(255), unique=True, nullable=False, index=True
    )
    hashed_password: Mapped[str] = mapped_column(
        String(255), nullable=False
    )


class Flight(Base):
    __tablename__ = "flights"
    __table_args__ = (
        Index("ix_flight_search", "departure", "arrival", "departure_time"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    flight_num: Mapped[str] = mapped_column(String(16), nullable=False)
    departure: Mapped[str] = mapped_column(String(64), nullable=False)
    arrival: Mapped[str] = mapped_column(String(64), nullable=False)
    departure_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Seat(Base):
    __tablename__ = "seats"
    __table_args__ = (
        UniqueConstraint("flight_id", "seat_code", name="uq_seat_flight_seat_code"),
        CheckConstraint("status IN (0, 1)", name="chk_seat_status"),
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
    __tablename__ = "orders"
    __table_args__ = (
        Index("ix_order_status_created", "status", "created_at"),
        # Prevent double-booking for active orders
        Index(
            "uq_flight_seat_active_order",
            "flight_id",
            "seat_code",
            unique=True,
            postgresql_where=text("status IN ('Pending', 'Confirmed')"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
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



class FlightResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    flight_num: str
    departure: str
    arrival: str
    departure_time: datetime


class SeatResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    seat_code: str
    status: int
    is_available: bool


class FlightSeatMapResponse(BaseModel):
    flight_id: uuid.UUID
    total_seats: int
    available_seats: int
    seats: list[SeatResponse]


class ReserveRequest(BaseModel):
    flight_id: uuid.UUID
    seat_code: str


class OrderResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    user_id: uuid.UUID
    flight_id: uuid.UUID
    seat_code: str
    status: str
    created_at: datetime


class RegisterRequest(BaseModel):
    email: EmailStr = Field(max_length=255)
    password: str = Field(min_length=6, max_length=72)


class LoginRequest(BaseModel):
    email: EmailStr = Field(max_length=255)
    password: str = Field(max_length=72)


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class LogoutRequest(BaseModel):
    refresh_token: str


class RefreshRequest(BaseModel):
    refresh_token: str

