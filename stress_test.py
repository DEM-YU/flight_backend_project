

import asyncio
import uuid
from collections import Counter

import aiohttp


BASE_URL    = "http://localhost:8080"
URL         = f"{BASE_URL}/api/v1/orders/reserve"

FLIGHT_ID   = "00000000-0000-0000-0000-000000000001"
SEAT_CODE   = "1A"
CONCURRENCY = 30



async def reserve(session: aiohttp.ClientSession, idx: int) -> str:
    payload = {"flight_id": FLIGHT_ID, "seat_code": SEAT_CODE}
    try:
        async with session.post(URL, json=payload) as resp:
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

    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [reserve(session, i) for i in range(1, CONCURRENCY + 1)]
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
