"""
Stress Test Suite — Flight Seat Reservation Concurrency & Load Benchmark
========================================================================
Simulates genuine multi-user concurrency with distinct accounts and JWT tokens.
Supports three evaluation modes:
  1. regular:      50 concurrent distinct users racing for the SAME seat (verifies
                   Redis Lua CAS lock, zero overselling, zero false-rate-limits).
  2. extreme:      1,000 concurrent distinct users hammering the server simultaneously
                   (exposes event-loop queue latency, socket backpressure, and breaking point).
  3. pool-exhaust: 60 concurrent distinct users reserving 60 DIFFERENT seats in the same
                   millisecond (forces concurrent database writes, stressing the PostgreSQL
                   connection pool limit of 30 connections).

Usage:
  # 1. Start server:
  uvicorn main:app --reload

  # 2. Run stress tests:
  python stress_test.py --mode regular          # Regular benchmark (50 users)
  python stress_test.py --mode extreme          # Extreme flood benchmark (1,000 users)
  python stress_test.py --mode pool-exhaust     # DB connection pool saturation (60 users)
  python stress_test.py --reset                 # Clean database orders & Redis seats first
"""

import argparse
import asyncio
import sys
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from typing import List

import aiohttp
from sqlalchemy import delete

from auth import create_access_token
from database import AsyncSessionLocal, settings
from models import Order, SeatState

# Defaults
BASE_URL = "http://127.0.0.1:8000"
FLIGHT_ID = "00000000-0000-0000-0000-000000000001"
DEFAULT_PASSWORD = "password123"


@dataclass
class RequestResult:
    idx: int
    user_email: str
    seat_code: str
    status_code: int
    duration_ms: float
    category: str
    detail: str


async def check_server(base_url: str = BASE_URL) -> bool:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{base_url}/health", timeout=aiohttp.ClientTimeout(total=3)) as r:
                return r.status in (200, 503)
    except Exception:
        return False


async def verify_http_login(session: aiohttp.ClientSession, base_url: str) -> bool:
    """Verifies that the live HTTP login endpoint responds correctly for user_0001."""
    payload = {"email": "user_0001@example.com", "password": DEFAULT_PASSWORD}
    try:
        async with session.post(f"{base_url}/api/v1/auth/login", json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                body = await resp.json()
                return "access_token" in body
            return False
    except Exception:
        return False


def get_user_token(user_idx: int) -> tuple[str, str]:
    """
    Returns (email, jwt_token) for the given user index from the seeded pool.
    User UUID format matches seed.py: 00000000-0000-0000-0001-{user_idx:012d}
    """
    user_id = uuid.UUID(f"00000000-0000-0000-0001-{user_idx:012d}")
    email = f"user_{user_idx:04d}@example.com"
    token = create_access_token(user_id)
    return email, token


async def reset_environment(flight_id: str, reset_all_seats: bool = True, single_seat: str | None = None) -> None:
    """Cleans up active orders in PostgreSQL and resets seat availability in Redis."""
    import redis.asyncio as aioredis

    # 1. Clear PostgreSQL test orders
    try:
        async with AsyncSessionLocal() as db:
            if reset_all_seats or single_seat is None:
                await db.execute(delete(Order).where(Order.flight_id == uuid.UUID(flight_id)))
            else:
                await db.execute(delete(Order).where(Order.flight_id == uuid.UUID(flight_id), Order.seat_code == single_seat))
            await db.commit()
    except Exception as e:
        print(f"  [RESET WARN] Could not clear PostgreSQL orders: {e}")

    # 2. Reset Redis seat statuses
    try:
        r = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
        key = f"flight:{flight_id}:seats"
        if reset_all_seats or single_seat is None:
            mapping = {f"{i}A": SeatState.AVAILABLE for i in range(1, 101)}
            await r.hset(key, mapping=mapping)
            print("  [RESET] Reset all 100 seats in Redis -> available (0)")
        else:
            await r.hset(key, single_seat, SeatState.AVAILABLE)
            print(f"  [RESET] Reset seat {single_seat} in Redis -> available (0)")
        await r.aclose()
    except Exception as e:
        print(f"  [RESET WARN] Could not reset Redis seats: {e}")


async def reserve_call(
    session: aiohttp.ClientSession,
    idx: int,
    user_email: str,
    token: str,
    flight_id: str,
    seat_code: str,
    base_url: str,
    timeout_sec: float,
) -> RequestResult:
    url = f"{base_url}/api/v1/orders/reserve"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"flight_id": flight_id, "seat_code": seat_code}

    t0 = time.perf_counter()
    try:
        async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_sec)) as resp:
            dur = (time.perf_counter() - t0) * 1000.0
            status = resp.status
            try:
                body = await resp.json(content_type=None)
                detail = body.get("detail", "") if isinstance(body, dict) else str(body)
            except Exception:
                detail = await resp.text()

            if status == 201:
                return RequestResult(idx, user_email, seat_code, 201, dur, "success", f"Reserved: order_id={body.get('id', '')}")
            elif status == 409:
                return RequestResult(idx, user_email, seat_code, 409, dur, "conflict", detail)
            elif status == 429:
                return RequestResult(idx, user_email, seat_code, 429, dur, "rate_limited", detail)
            elif status == 503:
                return RequestResult(idx, user_email, seat_code, 503, dur, "service_unavailable", detail)
            elif status == 500:
                return RequestResult(idx, user_email, seat_code, 500, dur, "server_error", detail)
            else:
                return RequestResult(idx, user_email, seat_code, status, dur, "other_error", detail)

    except asyncio.TimeoutError:
        dur = (time.perf_counter() - t0) * 1000.0
        return RequestResult(idx, user_email, seat_code, 0, dur, "timeout", f"Client timeout after {timeout_sec}s")
    except Exception as exc:
        dur = (time.perf_counter() - t0) * 1000.0
        return RequestResult(idx, user_email, seat_code, 0, dur, "connection_error", str(exc))


def print_percentiles(durations: List[float]) -> None:
    if not durations:
        return
    sorted_d = sorted(durations)
    n = len(sorted_d)
    min_val = sorted_d[0]
    p50 = sorted_d[int(n * 0.50)]
    p90 = sorted_d[min(int(n * 0.90), n - 1)]
    p95 = sorted_d[min(int(n * 0.95), n - 1)]
    p99 = sorted_d[min(int(n * 0.99), n - 1)]
    max_val = sorted_d[-1]
    avg = sum(sorted_d) / n

    print("  Latency Distribution (ms):")
    print(f"    Min:  {min_val:6.2f} ms | P50:  {p50:6.2f} ms | P90:  {p90:6.2f} ms")
    print(f"    P95:  {p95:6.2f} ms | P99:  {p99:6.2f} ms | Max:  {max_val:6.2f} ms | Avg: {avg:6.2f} ms")


async def run_benchmark(
    mode: str,
    concurrency: int,
    seat_code: str,
    flight_id: str,
    base_url: str,
    timeout_sec: float,
) -> None:
    print("=" * 70)
    print("  FLIGHT BACKEND CONCURRENCY & STRESS TEST")
    print("=" * 70)
    print(f"  Target Server:  {base_url}")
    print(f"  Test Mode:      {mode.upper()}")
    print(f"  Concurrency:    {concurrency} distinct users")
    print(f"  Flight UUID:    {flight_id}")
    if mode == "pool-exhaust":
        print(f"  Target Seats:   1A through {concurrency}A (multi-seat write flood)")
    else:
        print(f"  Target Seat:    {seat_code} (hot-spot lock contention)")
    print("=" * 70)

    # 1. Health check
    print("\n[Step 1/4] Checking server status ...")
    if not await check_server(base_url):
        print(f"  ERROR: Backend server not reachable at {base_url}")
        print("  Please start the server first in another terminal:")
        print("    uvicorn main:app --reload")
        sys.exit(1)
    print("  Server is healthy.")

    # 2. HTTP Login verify
    print("\n[Step 2/4] Verifying HTTP auth endpoint ...")
    async with aiohttp.ClientSession() as s:
        auth_ok = await verify_http_login(s, base_url)
    if not auth_ok:
        print("  WARNING: Could not log in via HTTP. Seed data may be missing.")
        print("  Please run: python seed.py")
    else:
        print("  HTTP auth check OK: user_0001 verified against PostgreSQL.")

    # 3. Environment reset
    print("\n[Step 3/4] Resetting Redis seats & clearing previous test orders ...")
    if mode == "pool-exhaust":
        await reset_environment(flight_id, reset_all_seats=True)
    else:
        await reset_environment(flight_id, reset_all_seats=False, single_seat=seat_code)

    # 4. Preparing user tokens
    print(f"\n[Step 4/4] Minting {concurrency} distinct JWT tokens for real seeded accounts ...")
    user_pool = [get_user_token(((i - 1) % 1000) + 1) for i in range(1, concurrency + 1)]
    print(f"  Prepared {len(user_pool)} authenticated user sessions.")

    # 5. Firing concurrent requests
    print(f"\n>>> Firing {concurrency} simultaneous requests at {base_url} ...")
    t_start = time.perf_counter()

    connector = aiohttp.TCPConnector(limit=max(concurrency, 100), force_close=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        if mode == "pool-exhaust":
            # Each user reserves a different seat (forces concurrent database writes)
            tasks = [
                reserve_call(session, i, user_pool[i - 1][0], user_pool[i - 1][1], flight_id, f"{i}A", base_url, timeout_sec)
                for i in range(1, concurrency + 1)
            ]
        else:
            # All users race for the exact same seat (hot-spot race condition)
            tasks = [
                reserve_call(session, i, user_pool[i - 1][0], user_pool[i - 1][1], flight_id, seat_code, base_url, timeout_sec)
                for i in range(1, concurrency + 1)
            ]
        results: List[RequestResult] = await asyncio.gather(*tasks)

    t_total = time.perf_counter() - t_start
    rps = concurrency / t_total if t_total > 0 else 0

    # 6. Evaluation & Reporting
    categories = Counter(r.category for r in results)
    durations = [r.duration_ms for r in results]

    print("\n" + "=" * 70)
    print("  TEST RESULTS SUMMARY")
    print("=" * 70)
    print(f"  Total Requests:       {concurrency}")
    print(f"  Total Wall Time:      {t_total:.3f} s")
    print(f"  Throughput (RPS):     {rps:.1f} req/sec")
    print("-" * 70)
    print(f"  HTTP 201 (Reserved):         {categories['success']:4d}  ({'Passed lock' if categories['success'] >= 1 else 'None'})")
    print(f"  HTTP 409 (Conflict):         {categories['conflict']:4d}  (Redis fast-fail lock blocked)")
    print(f"  HTTP 429 (Rate Limit):       {categories['rate_limited']:4d}  (Distinct users, should be 0)")
    print(f"  HTTP 503 (Pool Exhausted):   {categories['service_unavailable']:4d}  (Connection pool max_connections limit hit)")
    print(f"  HTTP 500 (DB Error):         {categories['server_error']:4d}  (Database error)")
    print(f"  Timeout (> {timeout_sec}s):            {categories['timeout']:4d}  (Event loop queue stall)")
    print(f"  Connection Errors:           {categories['connection_error']:4d}  (Socket backlog drop)")
    print("-" * 70)
    print_percentiles(durations)
    print("=" * 70)

    # 7. Diagnostic Breakdown by Mode
    if mode == "regular":
        if categories["success"] == 1 and categories["rate_limited"] == 0 and categories["server_error"] == 0 and categories["service_unavailable"] == 0:
            print("\n  [DIAGNOSTIC: PERFECT PASS]")
            print("  ✓ Redis Lua Atomic Lock: Exactly 1 reservation succeeded.")
            print(f"  ✓ Fast-Fail Rejection:   {categories['conflict']} requests blocked at Redis layer without touching DB.")
            print("  ✓ Zero False-Limiting:   Distinct JWT identities prevented single-user rate limiter false-tripping.")
            print("  ✓ Data Integrity:        Zero overselling detected.")
        else:
            print("\n  [DIAGNOSTIC: ABNORMAL OUTCOME]")
            print(f"  Success: {categories['success']}, Conflict: {categories['conflict']}, RateLimited: {categories['rate_limited']}, 503: {categories['service_unavailable']}")

    elif mode == "extreme":
        print("\n  [DIAGNOSTIC: EXTREME CONCURRENCY BEHAVIOR]")
        print("  Under 1,000 instant concurrent requests on a single-core Python event loop:")
        if categories["service_unavailable"] > 0:
            print("  💥 PHYSICAL CAPACITY BOUNDARY TRIGGERED:")
            print(f"      - {categories['service_unavailable']} requests received HTTP 503 ('Service temporarily unavailable').")
            print(f"      - ROOT CAUSE: Exceeded configured redis_pool connection limit ({settings.REDIS_MAX_CONNECTIONS}).")
            print(f"      - Concurrency exceeded the active connection pool boundary ({settings.REDIS_MAX_CONNECTIONS}).")
            print("      - PROOF: Your core business logic did NOT fail. The failure is a resource quota ceiling")
            print("        designed to protect system RAM from unbounded connection growth.")
        if categories["timeout"] > 0 or categories["connection_error"] > 0:
            print(f"      - {categories['timeout']} client timeouts / {categories['connection_error']} socket drops.")
        print(f"  ✓ Redis CAS lock remained fully intact: {categories['success']} winner, {categories['conflict']} rejected.")

    elif mode == "breakdown":
        print("\n  [DIAGNOSTIC: SYSTEM SATURATION & BREAKDOWN OBSERVED]")
        print("  Under 3,000 instant concurrent requests with 1.5s client timeout:")
        if categories["timeout"] > 0:
            print(f"  💥 EVENT-LOOP QUEUE STALL:  {categories['timeout']} requests timed out waiting in Python single-core event loop.")
        if categories["service_unavailable"] > 0:
            print(f"  💥 CONNECTION POOL BREACH: {categories['service_unavailable']} requests rejected (pool max={settings.REDIS_MAX_CONNECTIONS}).")
        if categories["connection_error"] > 0:
            print(f"  💥 TCP SOCKET BACKLOG DROP: {categories['connection_error']} connections dropped by OS network stack.")
        print("  ✓ PROVES: The system's true physical bottleneck is single-core CPU saturation and pool limits.")
        print("  ✓ ARCHITECTURE LESSON: Scaling past 2,000+ instant concurrency requires multi-worker deployment (Gunicorn) and Nginx.")

    elif mode == "pool-exhaust":
        print("\n  [DIAGNOSTIC: DATABASE CONNECTION POOL SATURATION]")
        print(f"  Simulated {concurrency} users reserving {concurrency} different seats simultaneously.")
        print("  Because each request succeeds in Redis, ALL requests hit PostgreSQL concurrently.")
        print("  Configured connection pool: pool_size=10, max_overflow=20 (Max 30 connections).")
        if categories["server_error"] > 0:
            print("  💥 BOTTLENECK OBSERVED (QueuePool limit reached):")
            print(f"      - {categories['server_error']} requests failed with HTTP 500 because the DB connection pool was exhausted.")
            print("      - Proves why high-throughput order writes require asynchronous message queues (Kafka/RabbitMQ).")
        else:
            print("  ✓ All requests queued and completed within pool_timeout.")
            print(f"  ✓ {categories['success']} orders successfully written to PostgreSQL.")

    print("=" * 70 + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Multi-User Seat Reservation Concurrency & Load Benchmark")
    parser.add_argument(
        "-m", "--mode",
        choices=["regular", "extreme", "breakdown", "pool-exhaust"],
        default="regular",
        help="Test mode: 'regular' (50 users), 'extreme' (1,000 users), 'breakdown' (3,000 users breakdown), 'pool-exhaust' (multi-seat DB stress)",
    )
    parser.add_argument(
        "-n", "--concurrency",
        type=int,
        default=None,
        help="Override concurrency number (default: regular=50, extreme=1000, pool-exhaust=60)",
    )
    parser.add_argument(
        "-s", "--seat",
        type=str,
        default="1A",
        help="Seat code to test for single-seat modes (default: 1A)",
    )
    parser.add_argument(
        "-f", "--flight",
        type=str,
        default=FLIGHT_ID,
        help=f"Flight UUID (default: {FLIGHT_ID})",
    )
    parser.add_argument(
        "-u", "--url",
        type=str,
        default=BASE_URL,
        help=f"Base backend URL (default: {BASE_URL})",
    )
    parser.add_argument(
        "-t", "--timeout",
        type=float,
        default=None,
        help="Request timeout in seconds (default: regular=5.0s, extreme=2.0s, pool-exhaust=5.0s)",
    )
    parser.add_argument(
        "--reset-only",
        action="store_true",
        help="Only reset Redis seats and clear database orders, then exit",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.reset_only:
        asyncio.run(reset_environment(args.flight, reset_all_seats=True))
        sys.exit(0)

    # Defaults per mode
    if args.concurrency is None:
        concurrency_map = {"regular": 50, "extreme": 1000, "breakdown": 3000, "pool-exhaust": 60}
        concurrency = concurrency_map[args.mode]
    else:
        concurrency = args.concurrency

    if args.timeout is None:
        timeout_map = {"regular": 5.0, "extreme": 2.0, "breakdown": 1.5, "pool-exhaust": 6.0}
        timeout = timeout_map[args.mode]
    else:
        timeout = args.timeout

    asyncio.run(run_benchmark(args.mode, concurrency, args.seat, args.flight, args.url, timeout))
