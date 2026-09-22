"""
Stress Test — Flight Seat Reservation Concurrency Test
=======================================================
Sends N concurrent reserve requests for the SAME seat to verify that
exactly 1 succeeds (no overselling).

Usage:
    # Make sure the server is running first:
    #   uvicorn main:app --reload
    #
    # Then in another terminal:
    python stress_test.py                     # default: 30 concurrent, seat 1A
    python stress_test.py -n 50               # 50 concurrent requests
    python stress_test.py -s 2A               # test seat 2A
    python stress_test.py --reset             # reset seat in Redis before test
"""

import argparse
import asyncio
import sys
from collections import Counter

import aiohttp


# Defaults

BASE_URL    = "http://127.0.0.1:8000"
FLIGHT_ID   = "00000000-0000-0000-0000-000000000001"
SEAT_CODE   = "1A"
CONCURRENCY = 30

LOGIN_URL   = f"{BASE_URL}/api/v1/auth/login"
RESERVE_URL = f"{BASE_URL}/api/v1/orders/reserve"
HEALTH_URL  = f"{BASE_URL}/health"

LOGIN_EMAIL    = "test@example.com"
LOGIN_PASSWORD = "password123"





async def check_server() -> bool:

    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(HEALTH_URL, timeout=aiohttp.ClientTimeout(total=3)) as r:
                return r.status in (200, 503)
    except Exception:
        return False


async def login(session: aiohttp.ClientSession) -> str | None:
    login_data = {"email": LOGIN_EMAIL, "password": LOGIN_PASSWORD}
    try:
        async with session.post(LOGIN_URL, json=payload) as resp:
            if resp.status == 200:
                body = await resp.json()
                return body["access_token"]
            else:
                text = await resp.text()
                print(f"  [LOGIN FAIL] HTTP {resp.status}: {text}")
                return None
    except Exception as e:
        print(f"  [LOGIN EXC] {e}")
        return None


async def reset_seat_via_redis(flight_id: str, seat_code: str) -> None:

    try:
        import redis.asyncio as aioredis
        from database import settings
        r = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
        key = f"flight:{flight_id}:seats"
        await r.hset(key, seat_code, "0")
        await r.aclose()
        print(f"  [RESET] Redis seat {seat_code} on flight {flight_id} -> available")
    except Exception as e:
        print(f"  [RESET WARN] Could not reset Redis seat: {e}")
        print("               (This is OK if Redis is not local or settings differ)")


async def reserve(
    session: aiohttp.ClientSession,
    idx: int,
    headers: dict,
    flight_id: str,
    seat_code: str,
) -> str:

    url = RESERVE_URL
    reserve_body = {"flight_id": flight_id, "seat_code": seat_code}
    try:
        async with session.post(url, json=reserve_body, headers=headers) as resp:
            status = resp.status
            body = await resp.json(content_type=None)
            if status == 201:
                print(f"  [OK]  [{idx:02d}] reserved, order_id={body.get('id')}")
                return "success"
            elif status == 409:
                print(f"  [409] [{idx:02d}] conflict: {body.get('detail')}")
                return "conflict"
            elif status == 404:
                print(f"  [404] [{idx:02d}] not found: {body.get('detail')}")
                return "error"
            else:
                print(f"  [ERR] [{idx:02d}] HTTP {status} | {body}")
                return "error"
    except Exception as exc:
        print(f"  [EXC] [{idx:02d}] {exc}")
        return "exception"



# Main

async def main(concurrency: int, seat_code: str, flight_id: str, reset: bool) -> None:
    print("=" * 60)
    print("  Flight Seat Reservation — Stress Test")
    print("=" * 60)
    print(f"  Server:      {BASE_URL}")
    print(f"  Flight ID:   {flight_id}")
    print(f"  Seat Code:   {seat_code}")
    print(f"  Concurrency: {concurrency}")
    print(f"  Reset Seat:  {'Yes' if reset else 'No'}")
    print("=" * 60)


    print("\n[1/4] Checking server availability ...")
    alive = await check_server()
    if not alive:
        print("\n  ERROR: Server is not reachable at", BASE_URL)
        print("  Please start the server first:")
        print("    uvicorn main:app --reload")
        print()
        sys.exit(1)
    print("  Server is online.\n")


    if reset:
        print("[2/4] Resetting seat in Redis ...")
        await reset_seat_via_redis(flight_id, seat_code)
        print()
    else:
        print("[2/4] Skipping seat reset (use --reset to enable).\n")


    print("[3/4] Logging in as test user ...")
    async with aiohttp.ClientSession() as login_session:
        token = await login(login_session)
    if not token:
        print("\n  ERROR: Could not obtain access token. Aborting.")
        print("  Make sure you ran: python seed.py")
        sys.exit(1)
    headers = {"Authorization": f"Bearer {token}"}
    print("  Login OK. Token acquired.\n")


    print(f"[4/4] Firing {concurrency} concurrent reserve requests ...")
    print("-" * 60)
    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            reserve(session, i, headers, flight_id, seat_code)
            for i in range(1, concurrency + 1)
        ]
        results = await asyncio.gather(*tasks)


    counts = Counter(results)
    print()
    print("=" * 60)
    print(f"  Success:   {counts['success']}")
    print(f"  Conflict:  {counts['conflict']}")
    print(f"  Error:     {counts['error']}")
    print(f"  Exception: {counts['exception']}")
    print("=" * 60)

    if counts["success"] == 1:
        print("  RESULT: PASS — exactly 1 reservation succeeded.")
    elif counts["success"] == 0:
        print("  RESULT: FAIL — no reservations succeeded. Check server logs.")
    else:
        print(f"  RESULT: FAIL — {counts['success']} succeeded, OVERSELLING detected!")
    print("=" * 60)
    print()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stress test for seat reservation")
    parser.add_argument("-n", "--concurrency", type=int, default=CONCURRENCY,
                        help=f"Number of concurrent requests (default: {CONCURRENCY})")
    parser.add_argument("-s", "--seat", type=str, default=SEAT_CODE,
                        help=f"Seat code to test (default: {SEAT_CODE})")
    parser.add_argument("-f", "--flight", type=str, default=FLIGHT_ID,
                        help=f"Flight UUID (default: {FLIGHT_ID})")
    parser.add_argument("--reset", action="store_true",
                        help="Reset the seat in Redis before testing (makes test repeatable)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args.concurrency, args.seat, args.flight, args.reset))
