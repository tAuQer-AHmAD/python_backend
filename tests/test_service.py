import asyncio

import pytest

from app.service import JobService


@pytest.mark.anyio
async def test_worker_pool_processes_multiple_jobs():
    service = JobService(worker_count=2, queue_capacity=10)
    await service.start()

    try:
        jobs = [
            await service.create_job("one"),
            await service.create_job("two"),
            await service.create_job("three"),
        ]

        await service.queue.join()

        assert all(job.status == "completed" for job in jobs)
    finally:
        await service.stop()


@pytest.mark.anyio
async def test_queue_has_finite_capacity():
    service = JobService(worker_count=0, queue_capacity=1)
    await service.start()

    try:
        await service.create_job("first")
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(service.create_job("second"), timeout=0.05)
    finally:
        await service.stop()
