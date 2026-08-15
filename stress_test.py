

import asyncio
import uuid
from collections import Counter

import aiohttp


BASE_URL    = "http://localhost:8000"
URL         = f"{BASE_URL}/api/v1/orders/reserve"

FLIGHT_ID   = "00000000-0000-0000-0000-000000000001"
SEAT_CODE   = "1A"
CONCURRENCY = 30



async def reserve(session: aiohttp.ClientSession, idx: int, headers: dict) -> str:
    payload = {"flight_id": FLIGHT_ID, "seat_code": SEAT_CODE}
    try:
        async with session.post(URL, json=payload, headers=headers) as resp:
            status = resp.status
            body   = await resp.json(content_type=None)
            if status == 201:
                print(f"  [OK]  [{idx:02d}] reserved, order_id={body.get('id')}")
                return "success"
            elif status == 409:
                print(f"  [409] [{idx:02d}] conflict: {body.get('detail')}")
                return "conflict"
            else:
                print(f"  [ERR] [{idx:02d}] HTTP {status} | {body}")
                return "error"
    except Exception as exc:
        print(f"  [EXC] [{idx:02d}] {exc}")
        return "exception"



async def main() -> None:
    print(f"Target: {URL}  Seat: {SEAT_CODE}  Concurrency: {CONCURRENCY}")
    print("-" * 60)

    # 1. Log in to get JWT access token
    login_url = f"{BASE_URL}/api/v1/auth/login"
    login_payload = {"email": "test@example.com", "password": "password123"}
    headers = {}
    async with aiohttp.ClientSession() as login_session:
        try:
            async with login_session.post(login_url, json=login_payload) as login_resp:
                if login_resp.status != 200:
                    body = await login_resp.text()
                    print(f"FAILED to log in: {login_resp.status} - {body}")
                    return
                res = await login_resp.json()
                access_token = res["access_token"]
                headers = {"Authorization": f"Bearer {access_token}"}
                print("[OK] Logged in successfully. Token acquired.")
        except Exception as e:
            print(f"EXC during login: {e}")
            return

    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [reserve(session, i, headers) for i in range(1, CONCURRENCY + 1)]
        results = await asyncio.gather(*tasks)


    counts = Counter(results)
    print()
    print("-" * 60)
    print(f"Success: {counts['success']}  Conflict: {counts['conflict']}  "
          f"Error: {counts['error']}  Exception: {counts['exception']}")
    print("-" * 60)


    if counts["success"] == 1:
        print("PASS: exactly 1 reservation succeeded.")
    elif counts["success"] == 0:
        print("FAIL: no reservations succeeded. Check server logs.")
    else:
        print(f"FAIL: {counts['success']} reservations succeeded, overselling detected.")


if __name__ == "__main__":
    asyncio.run(main())
