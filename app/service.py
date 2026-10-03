import asyncio
import dataclasses
import logging
import uuid
from collections.abc import Awaitable, Callable

from .models import Job, JobStatus
from .processor import process_payload

logger = logging.getLogger(__name__)

Processor = Callable[[str], Awaitable[None]]

# Messages exposed to API clients; the underlying exceptions are only logged.
PROCESSING_FAILED = "processing failed"
SHUTDOWN_BEFORE_COMPLETION = "service shut down before the job completed"


class JobServiceError(Exception):
    """Base class for errors raised by JobService."""


class QueueFullError(JobServiceError):
    """The job queue is at capacity."""


class ServiceNotRunningError(JobServiceError):
    """The service is not started, or is shutting down."""


class JobService:
    """In-memory job store with a fixed-size worker pool.

    All state lives on a single asyncio event loop. Every state transition
    is done without awaiting in between, so workers and request handlers
    never observe a partially-updated job and no lock is needed. Callers
    only ever receive copies of jobs, never the shared instances.
    """

    def __init__(
        self,
        worker_count: int = 2,
        queue_capacity: int = 10,
        processor: Processor = process_payload,
        shutdown_timeout: float = 5.0,
    ):
        if worker_count < 0:
            raise ValueError("worker_count must be >= 0")
        # asyncio.Queue treats maxsize <= 0 as unbounded.
        if queue_capacity < 1:
            raise ValueError("queue_capacity must be >= 1")
        if shutdown_timeout < 0:
            raise ValueError("shutdown_timeout must be >= 0")

        self.worker_count = worker_count
        self.shutdown_timeout = shutdown_timeout
        self.queue: asyncio.Queue[str] = asyncio.Queue(maxsize=queue_capacity)
        self.jobs: dict[str, Job] = {}
        self.workers: list[asyncio.Task] = []
        self._processor = processor
        self._accepting = False
        self._started = False

    @property
    def accepting(self) -> bool:
        return self._accepting

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._accepting = True
        self.workers = [
            asyncio.create_task(self._worker(), name=f"job-worker-{i}")
            for i in range(self.worker_count)
        ]

    async def stop(self, timeout: float | None = None) -> None:
        """Stop accepting jobs, drain for up to `timeout` seconds, then cancel.

        Jobs that have not finished when the grace period ends are marked
        failed, so no job is left queued or processing forever.
        """
        if not self._accepting:
            return
        self._accepting = False
        timeout = self.shutdown_timeout if timeout is None else timeout

        if self.workers:
            try:
                await asyncio.wait_for(self.queue.join(), timeout)
            except asyncio.TimeoutError:
                logger.warning("shutdown grace period expired; cancelling workers")

        for worker in self.workers:
            worker.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)
        self.workers.clear()

        while not self.queue.empty():
            job = self.jobs[self.queue.get_nowait()]
            self._fail(job, SHUTDOWN_BEFORE_COMPLETION)
            self.queue.task_done()

    async def create_job(self, payload: str) -> Job:
        if not self._accepting:
            raise ServiceNotRunningError("service is not accepting jobs")

        job = Job(id=str(uuid.uuid4()), payload=payload)
        try:
            self.queue.put_nowait(job.id)
        except asyncio.QueueFull:
            raise QueueFullError("job queue is full") from None
        # No await between enqueue and store, so a worker can't see the id first.
        self.jobs[job.id] = job
        return dataclasses.replace(job)

    def get_job(self, job_id: str) -> Job | None:
        job = self.jobs.get(job_id)
        return dataclasses.replace(job) if job else None

    async def _worker(self) -> None:
        while True:
            job_id = await self.queue.get()
            try:
                await self._process(self.jobs[job_id])
            except Exception:
                logger.exception("worker failed handling job %s", job_id)
            finally:
                self.queue.task_done()

    async def _process(self, job: Job) -> None:
        job.status = JobStatus.PROCESSING
        try:
            await self._processor(job.payload)
        except asyncio.CancelledError:
            self._fail(job, SHUTDOWN_BEFORE_COMPLETION)
            raise
        except Exception:
            logger.exception("job %s failed", job.id)
            self._fail(job, PROCESSING_FAILED)
        else:
            job.status = JobStatus.COMPLETED

    @staticmethod
    def _fail(job: Job, message: str) -> None:
        job.status = JobStatus.FAILED
        job.error = message
