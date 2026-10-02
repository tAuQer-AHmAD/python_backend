import asyncio
import uuid

from .models import Job
from .processor import process_payload


class JobService:
    def __init__(self, worker_count: int = 2, queue_capacity: int = 10):
        self.worker_count = worker_count
        self.queue: asyncio.Queue[str] = asyncio.Queue(maxsize=queue_capacity)
        self.jobs: dict[str, Job] = {}
        self.workers: list[asyncio.Task] = []
        self.stopping = False

    async def start(self) -> None:
        for _ in range(self.worker_count):
            self.workers.append(asyncio.create_task(self._worker()))

    async def stop(self) -> None:
        self.stopping = True
        for worker in self.workers:
            worker.cancel()

    async def create_job(self, payload: str) -> Job:
        job = Job(id=str(uuid.uuid4()), payload=payload)
        self.jobs[job.id] = job
        await self.queue.put(job.id)
        return job

    def get_job(self, job_id: str) -> Job | None:
        return self.jobs.get(job_id)

    async def _worker(self) -> None:
        while True:
            job_id = await self.queue.get()
            job = self.jobs[job_id]
            job.status = "processing"

            try:
                await process_payload(job.payload)
                job.status = "completed"
            except Exception as exc:
                job.status = "failed"
                job.error = str(exc)
            finally:
                self.queue.task_done()
