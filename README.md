# Flight Booking Backend

A flight search and seat reservation backend built with **FastAPI**, **PostgreSQL**, and **Redis**. Focuses on atomic seat reservations to prevent overselling under concurrent load, cache-aside querying, and JWT authentication with Redis-backed session revocation.

---

## Tech Stack

| Technology | Role |
| :--- | :--- |
| **FastAPI** | Asynchronous web framework (Python 3.12) |
| **PostgreSQL** | Relational database for storing users, flight metadata, and orders |
| **Redis** | In-memory store for atomic seat locks and route cache |
| **SQLAlchemy (Async)** | Async ORM for database operations |
| **PyJWT & Bcrypt** | Password hashing and JWT generation |
| **Docker Compose** | Local container setup for PostgreSQL and Redis |

---

## System Architecture & Workflows

### 1. Atomic Seat Reservation (Lua Lock + PG Transaction)

To prevent overselling under concurrent requests, seat availability checks and locking are decoupled from the database and handled in Redis using a **Lua script**. If the database transaction fails to commit (e.g. connection drops or constraint violations), the backend issues a **compensating write** in Redis to release the seat.

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant API as FastAPI Router
    participant Redis as Redis Cache
    participant DB as PostgreSQL

    Client->>API: POST /api/v1/orders/reserve {flight_id, seat_code} (JWT Authorized)
    Note over API: Extract user_id from JWT
    API->>Redis: EVAL RESERVE_SEAT_SCRIPT (Lua check-and-set)
    alt Seat Not Found (-1)
        Redis-->>API: Return -1
        API-->>Client: HTTP 404 (Seat Not Found)
    else Seat Unavailable (0)
        Redis-->>API: Return 0
        API-->>Client: HTTP 409 (Seat Unavailable)
    else Seat Locked Successfully (1)
        Redis-->>API: Return 1 (Seat marked '1' in Hash)
        
        Note over API: Start PostgreSQL Transaction
        API->>DB: INSERT INTO orders (id, user_id, flight_id, seat_code, status='Pending')
        
        alt PG Commit Success
            DB-->>API: Transaction Committed
            API->>Redis: ZADD order:timeout:zset (Schedule 60s Expiration)
            API-->>Client: HTTP 201 Created (Order Response)
        else PG Commit Failure (Database down / exception)
            DB-->>API: DB Exception & Rollback
            Note over API: Compensating Rollback Active
            API->>Redis: HSET flight:{id}:seats {seat_code} '0' (Unlock Seat)
            API-->>Client: HTTP 500 Internal Server Error
        end
    end
```

### 2. Redis ZSet Delay Queue for Order Expiration

Unpaid orders expire after **60 seconds**. Rather than relying on in-process sleep tasks that are lost upon server restarts, expiration timestamps are stored in a **Redis Sorted Set (ZSet)** delay queue (`score = unix_timestamp + 60s`). A background worker periodically pops due orders via a Lua script (`ZRANGEBYSCORE` + `ZREM`), cancels unpaid orders in PostgreSQL, and releases the Redis seat lock with linear backoff retries.

```mermaid
graph TD
    A[Order Created: Status = Pending] --> B["Register in Redis ZSet delay queue (score = now + 60s)"]
    B --> C["Background Worker polls ZSet atomically via Lua script"]
    C -->|Order Due & Popped| D[Fetch Order Status via isolated DB Session]
    D -->|Status is CONFIRMED/CANCELLED| E[No Action Needed - Discard]
    D -->|Status is still PENDING| F[Update Order Status to CANCELLED in DB]
    F --> G["Release Redis Lock: set seat to '0'"]
    G -->|Success| H[Lock Released Successfully]
    G -->|Failure due to network/Redis blip| I[Linear Backoff Retry]
    I -->|Attempt 1-3 Success| H
    I -->|All Retries Exhausted| J[CRITICAL Alarm: Log Ghost Lock for Manual Audit]
```

---

## Design Decisions & Concurrency Handling

### 1. Atomic Locking via Redis Lua Script
* **Context:** In a high-concurrency booking scenario, database transactions (`SELECT ... FOR UPDATE`) lead to lock contention and connection pool exhaustion. Using separate Redis `GET` and `SET` calls introduces race conditions.
* **Approach:** Check-and-set logic runs in a single Redis Lua script:
  ```lua
  local status = redis.call('HGET', KEYS[1], ARGV[1])
  if status == false then return -1 end       -- Seat does not exist
  if status == '0' then 
      redis.call('HSET', KEYS[1], ARGV[1], '1') -- Lock the seat atomically
      return 1 
  else 
      return 0                                -- Already locked
  end
  ```
  This avoids race conditions and moves lock contention out of PostgreSQL.

### 2. Dual-Write Rollback (Preventing Ghost Locks)
* **Problem:** If a seat is locked in Redis but the subsequent PostgreSQL write fails, the seat remains locked in Redis with no matching order in the database ("ghost lock").
* **Solution:** The database insert is wrapped in a `try...except` block. If the database commit fails, the transaction is rolled back and a compensating `HSET` resets the seat back to available (`0`) in Redis.

### 3. Flight Search Caching (Cache-Aside)
* **Cache-Aside:** Flight search queries check Redis first and fall back to PostgreSQL on a cache miss.
* **Cache Stampede Prevention:** To prevent simultaneous cache expiration across popular routes, a random jitter of ±300s (5 minutes) is added to the 30-minute base TTL.
* **Negative Caching:** Routes with no scheduled flights are cached with a short 60-second TTL to avoid repeated database lookups for empty search results.

### 4. JWT Authentication with Refresh Token Revocation
* **Token Pair:** Login issues a short-lived Access Token (15 minutes) and a long-lived Refresh Token (7 days).
* **Revocation via Redis:** Refresh tokens are stored in Redis (`refresh_token:<token>`). Calling `/auth/logout` deletes the key from Redis. During token refresh, the endpoint validates both the JWT signature and its existence in Redis, allowing active revocation before token expiry.

### 5. Sliding-Window Rate Limiting
* Protects `/orders/reserve` against rapid-fire requests using a Redis ZSet sliding-window script.
* Identifies clients by JWT user ID or client IP, returning HTTP 429 with `Retry-After` headers when limits are exceeded.

---

## Project Structure

```
flight_backend_project/
├── main.py          # FastAPI application, exception handlers, and lifespan
├── api.py           # Flight search and seat reservation endpoints
├── auth_api.py      # Registration, login, logout, and token refresh endpoints
├── services.py      # Reservation logic, Redis Lua scripts, and timeout worker
├── rate_limiter.py  # Sliding-window rate limiting dependency via Redis ZSet
├── models.py        # SQLAlchemy models and Pydantic schemas
├── auth.py          # Password hashing (bcrypt) and JWT utilities
├── database.py      # Database engines, session makers, and settings
├── seed.py          # Database seeder initializing flights and seats for testing
├── stress_test.py   # Concurrency test script for overselling verification
└── tests/
    ├── test_auth.py         # Authentication and token lifecycle tests
    ├── test_services.py     # Unit tests for reservation, locking, and timeout
    └── test_rate_limiter.py # Unit and integration tests for rate limiting
```

---

## Core API Endpoints

| Method | Endpoint | Description | Auth Required |
| :--- | :--- | :--- | :--- |
| **POST** | `/api/v1/auth/register` | Register a new user account | None |
| **POST** | `/api/v1/auth/login` | Authenticate and return Access + Refresh Token | None |
| **POST** | `/api/v1/auth/logout` | Revoke user session and delete Refresh Token from Redis | **Refresh Token** |
| **POST** | `/api/v1/auth/refresh` | Reissue Access Token using a valid Refresh Token | None |
| **GET** | `/health` | Health check endpoint returning database and Redis connectivity | None |
| **GET** | `/api/v1/flights` | Search flights (Cache-Aside, returns `X-Cache-Status` header) | None |
| **POST** | `/api/v1/orders/reserve` | Reserve seat atomically (Returns order details, schedules timeout) | **JWT Access Token** |
| **POST** | `/api/v1/orders/{order_id}/pay` | Confirm payment for order, update status to Confirmed, and mark seat occupied in DB | **JWT Access Token** |

---

## Local Setup

### Prerequisites
* Docker & Docker Compose
* Python 3.12+

### 1. Start PostgreSQL & Redis
```bash
docker compose up -d
```

### 2. Configure Environment Variables
```bash
cp .env.example .env
```

### 3. Install Python Dependencies
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Seed Data & Start the Server
Seed a test flight with 30 seats and start the development server:
```bash
python seed.py
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```
* Interactive API Docs: `http://localhost:8000/docs`

### 5. Run Tests
```bash
pytest -v
```

---

## Concurrency Testing

A concurrency script verifies that concurrent requests for the same seat result in exactly one successful reservation:

```bash
python stress_test.py
```

**Test Flow:**
1. Logs in with the test user (`test@example.com`) to retrieve a JWT Access Token.
2. Fires **30 concurrent async requests** for the same seat (`1A`) on flight `00000000-0000-0000-0000-000000000001`.
3. Verifies that exactly 1 request succeeds (HTTP 201), while the remaining requests receive conflict (HTTP 409) or rate limited (HTTP 429).

**Sample Output:**
```text
============================================================
  Flight Seat Reservation — Stress Test
============================================================
  Server:      http://127.0.0.1:8000
  Flight ID:   00000000-0000-0000-0000-000000000001
  Seat Code:   1A
  Concurrency: 30
  Reset Seat:  No
============================================================

[1/4] Checking server availability ...
  Server is online.

[2/4] Skipping seat reset (use --reset to enable).

[3/4] Logging in as test user ...
  Login OK. Token acquired.

[4/4] Firing 30 concurrent reserve requests ...
------------------------------------------------------------
  [OK]  [01] reserved, order_id=...
  [409] [02] conflict: Seat is already reserved or unavailable.
  ...
  [429] [11] rate limited: Too many requests. Please try again later.
  ...
============================================================
  Success:      1
  Conflict:     9
  Rate Limited: 20
  Error:        0
  Exception:    0
============================================================
  RESULT: PASS — exactly 1 reservation succeeded.
============================================================
```
