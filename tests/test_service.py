import asyncio

import pytest

from app.models import JobStatus
from app.service import (
    PROCESSING_FAILED,
    SHUTDOWN_BEFORE_COMPLETION,
    JobService,
    QueueFullError,
    ServiceNotRunningError,
)

from .conftest import WAIT_TIMEOUT

pytestmark = pytest.mark.anyio


async def wait_idle(service: JobService) -> None:
    await asyncio.wait_for(service.queue.join(), WAIT_TIMEOUT)


async def test_worker_pool_processes_multiple_jobs():
    service = JobService(worker_count=2, queue_capacity=10)
    await service.start()

    try:
        jobs = [
            await service.create_job("one"),
            await service.create_job("two"),
            await service.create_job("three"),
        ]

        await wait_idle(service)

        assert all(service.get_job(job.id).status == JobStatus.COMPLETED for job in jobs)
    finally:
        await service.stop()


async def test_workers_process_jobs_concurrently(processor):
    service = JobService(worker_count=2, queue_capacity=10, processor=processor)
    await service.start()

    try:
        jobs = [await service.create_job(p) for p in ("one", "two", "three")]

        # Both workers pick up a job while neither can finish: true concurrency.
        await processor.wait_started(2)
        statuses = [service.get_job(job.id).status for job in jobs]
        assert statuses == [JobStatus.PROCESSING, JobStatus.PROCESSING, JobStatus.QUEUED]

        processor.release.set()
        await wait_idle(service)
        assert all(service.get_job(job.id).status == JobStatus.COMPLETED for job in jobs)
    finally:
        await service.stop()


async def test_worker_count_bounds_concurrency(processor):
    service = JobService(worker_count=3, queue_capacity=20, processor=processor)
    await service.start()

    try:
        for i in range(10):
            await service.create_job(f"job-{i}")
        await processor.wait_started(3)
        # Let every other runnable task run; no fourth job may start.
        for _ in range(10):
            await asyncio.sleep(0)
        assert len(processor.started) == 3
        assert len(service.workers) == 3
    finally:
        processor.release.set()
        await service.stop()


async def test_processing_error_marks_job_failed_and_worker_survives(processor):
    service = JobService(worker_count=1, queue_capacity=10, processor=processor)
    await service.start()

    try:
        failing = await service.create_job("please fail")
        ok = await service.create_job("fine")
        processor.release.set()
        await wait_idle(service)

        failed_job = service.get_job(failing.id)
        assert failed_job.status == JobStatus.FAILED
        # The internal exception text is logged, not exposed.
        assert failed_job.error == PROCESSING_FAILED
        assert service.get_job(ok.id).status == JobStatus.COMPLETED
    finally:
        await service.stop()


async def test_real_processor_fails_payloads_containing_fail():
    service = JobService(worker_count=1, queue_capacity=10)
    await service.start()

    try:
        job = await service.create_job("this should fail")
        await wait_idle(service)
        assert service.get_job(job.id).status == JobStatus.FAILED
    finally:
        await service.stop()


async def test_queue_full_is_rejected_immediately():
    service = JobService(worker_count=0, queue_capacity=1)
    await service.start()

    try:
        await service.create_job("first")
        with pytest.raises(QueueFullError):
            await service.create_job("second")
        # The rejected job must not linger as "queued".
        assert len(service.jobs) == 1
    finally:
        await service.stop()


async def test_returned_jobs_are_copies(processor):
    service = JobService(worker_count=1, queue_capacity=10, processor=processor)
    await service.start()

    try:
        job = await service.create_job("hello")
        job.status = JobStatus.COMPLETED
        assert service.get_job(job.id).status != JobStatus.COMPLETED
    finally:
        processor.release.set()
        await service.stop()


async def test_concurrent_creates_produce_unique_consistent_jobs(processor):
    service = JobService(worker_count=4, queue_capacity=100, processor=processor)
    await service.start()

    try:
        jobs = await asyncio.gather(*(service.create_job(f"job-{i}") for i in range(100)))
        assert len({job.id for job in jobs}) == 100

        processor.release.set()
        await wait_idle(service)
        assert all(job.status == JobStatus.COMPLETED for job in service.jobs.values())
    finally:
        await service.stop()


async def test_create_job_requires_running_service():
    service = JobService(worker_count=1, queue_capacity=1)
    with pytest.raises(ServiceNotRunningError):
        await service.create_job("too early")

    await service.start()
    await service.stop()
    with pytest.raises(ServiceNotRunningError):
        await service.create_job("too late")


async def test_start_is_idempotent():
    service = JobService(worker_count=2, queue_capacity=1)
    await service.start()
    await service.start()

    try:
        assert len(service.workers) == 2
    finally:
        await service.stop()


async def test_stop_drains_queued_jobs_within_grace_period(processor):
    service = JobService(
        worker_count=1, queue_capacity=10, processor=processor, shutdown_timeout=WAIT_TIMEOUT
    )
    await service.start()
    jobs = [await service.create_job(p) for p in ("one", "two")]
    await processor.wait_started(1)

    stopping = asyncio.create_task(service.stop())
    await asyncio.sleep(0)
    assert not service.accepting

    processor.release.set()
    await asyncio.wait_for(stopping, WAIT_TIMEOUT)

    assert all(service.get_job(job.id).status == JobStatus.COMPLETED for job in jobs)
    assert service.workers == []


async def test_stop_is_bounded_when_jobs_hang(processor):
    service = JobService(
        worker_count=1, queue_capacity=10, processor=processor, shutdown_timeout=0.01
    )
    await service.start()
    in_flight = await service.create_job("hangs forever")
    queued = await service.create_job("never started")
    await processor.wait_started(1)
    workers = list(service.workers)

    # The gate is never released; stop must still return.
    await asyncio.wait_for(service.stop(), WAIT_TIMEOUT)

    assert all(worker.done() for worker in workers)
    for job_id in (in_flight.id, queued.id):
        job = service.get_job(job_id)
        assert job.status == JobStatus.FAILED
        assert job.error == SHUTDOWN_BEFORE_COMPLETION


async def test_stop_is_idempotent():
    service = JobService(worker_count=1, queue_capacity=1)
    await service.start()
    await service.stop()
    await service.stop()


@pytest.mark.parametrize(
    "kwargs",
    [{"worker_count": -1}, {"queue_capacity": 0}, {"shutdown_timeout": -1}],
)
def test_invalid_configuration_is_rejected(kwargs):
    with pytest.raises(ValueError):
        JobService(**kwargs)
