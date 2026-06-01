# Flight Booking Engine

A high-concurrency transatlantic flight booking engine built on FastAPI, PostgreSQL, and Redis, designed to resolve strict eventual consistency and over-selling challenges.

## Architecture & Engineering Decisions

### 1. Concurrency & Inventory Control
- **Atomic Lua Locks:** Seat reservations bypass DB pessimistic locks and Redis `WATCH/MULTI` blocks in favor of a single-trip Lua script (`HGET` + `HSET`). This eliminates race condition windows entirely while maximizing throughput on the Redis single-threaded event loop.
- **Ghost Lock Compensation:** The system follows a "cache-first, DB-second" write pattern. To prevent "ghost locks" where a seat is locked in Redis but the subsequent PostgreSQL transaction fails, the exception handler enforces an explicit Redis state rollback alongside the DB rollback.

### 2. Performance & Query Optimization
- **Cache Avalanche Mitigation:** Route queries use a Cache-Aside pattern. To prevent thundering herd scenarios upon mass key expiration, the base TTL is injected with a randomized jitter (±300s).
- **Cache Penetration Defense:** Queries returning empty sets from the DB are explicitly cached with a short TTL (60s). This absorbs high-frequency malicious queries for non-existent routes without delaying the visibility of newly inserted flights.

### 3. State Lifecycle & Asynchronous Compensation
- **Isolated Timeout State Machine:** Unpaid orders are auto-cancelled via FastAPI `BackgroundTasks`. After a 60-second delay, the task spins up an isolated `AsyncSession` to transition the `PENDING` order to `CANCELLED`.
- **Exponential Backoff for Lock Release:** Releasing the Redis seat lock post-cancellation utilizes a linear backoff retry mechanism to survive transient network partitions. Exhausting retries without success triggers a `CRITICAL` log for manual intervention rather than infinitely looping.

## Quick Start

```bash
docker-compose up -d
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python seed.py
uvicorn main:app --host 0.0.0.0 --port 8080
```

## Load Test

```bash
python stress_test.py
```
Expected result: Exactly 1 request succeeds (HTTP 201) and all other concurrent requests are rejected (HTTP 409) with zero over-selling.
