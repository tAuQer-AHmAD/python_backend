import asyncio


async def process_payload(payload: str) -> None:
    # Simulated asynchronous work.
    await asyncio.sleep(0.02)

    if "fail" in payload:
        raise RuntimeError("simulated processing failure")
