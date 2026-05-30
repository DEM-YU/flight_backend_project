"""
stress_test.py — Concurrent Seat Reservation Stress Test

Target Endpoint: POST http://localhost:8080/api/v1/orders/reserve
Scenario: 30 concurrent coroutines attempting to reserve the exact same seat simultaneously.
Goal: Verify the efficacy of the Redis Lua atomic locking mechanism to prevent overselling.

Usage:
  python stress_test.py
"""

import asyncio
import uuid
from collections import Counter

import aiohttp

# Configuration
BASE_URL    = "http://localhost:8080"
URL         = f"{BASE_URL}/api/v1/orders/reserve"

# Must match the FLIGHT_ID defined in seed.py
FLIGHT_ID   = "00000000-0000-0000-0000-000000000001"
SEAT_CODE   = "1A"          # All concurrent requests target this single seat
CONCURRENCY = 30            # Number of concurrent requests


# Single Request Task
async def reserve(session: aiohttp.ClientSession, idx: int) -> str:
    payload = {"flight_id": FLIGHT_ID, "seat_code": SEAT_CODE}
    try:
        async with session.post(URL, json=payload) as resp:
            status = resp.status
            body   = await resp.json(content_type=None)
            if status == 201:
                print(f"  ✅ [{idx:02d}] Reservation Successful -> order_id={body.get('id')}")
                return "success"
            elif status == 409:
                print(f"  ❌ [{idx:02d}] Seat Already Occupied  -> {body.get('detail')}")
                return "conflict"
            else:
                print(f"  ⚠️  [{idx:02d}] Unexpected Response    -> HTTP {status} | {body}")
                return "error"
    except Exception as exc:
        print(f"  💥 [{idx:02d}] Request Exception       -> {exc}")
        return "exception"


# Main Stress Testing Logic
async def main() -> None:
    print("=" * 60)
    print(f"Concurrent Stress Test Started")
    print(f"Target Endpoint    : {URL}")
    print(f"Flight ID          : {FLIGHT_ID}")
    print(f"   Target Seat        : {SEAT_CODE}")
    print(f"   Concurrency Level  : {CONCURRENCY}")
    print("=" * 60)

    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [reserve(session, i) for i in range(1, CONCURRENCY + 1)]
        results = await asyncio.gather(*tasks)

    # Aggregate Statistics
    counts = Counter(results)
    print()
    print("=" * 60)
    print("Stress Test Results Summary")
    print(f"Successful Bookings (201) : {counts['success']:>3} times")
    print(f"Seat Conflicts (409)      : {counts['conflict']:>3} times")
    print(f"Unexpected Responses     : {counts['error']:>3} times")
    print(f"Request Exceptions        : {counts['exception']:>3} times")
    print("=" * 60)

    # Anti-overselling Assertion
    if counts["success"] == 1:
        print("Anti-overselling validation PASSED! Redis Lua atomic lock is functioning correctly.")
    elif counts["success"] == 0:
        print("No requests succeeded. Please check the server logs for details.")
    else:
        print(f"🔴 Anti-overselling validation FAILED! {counts['success']} requests succeeded, indicating overselling risk!")


if __name__ == "__main__":
    asyncio.run(main())
