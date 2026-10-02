from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from .service import JobService


service = JobService(worker_count=2, queue_capacity=3)


class CreateJobRequest(BaseModel):
    payload: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    await service.start()
    yield
    await service.stop()


app = FastAPI(title="Job Processing Service", lifespan=lifespan)


@app.post("/jobs", status_code=201)
async def create_job(request: CreateJobRequest):
    if not request.payload.strip():
        raise HTTPException(status_code=400, detail="payload is required")

    job = await service.create_job(request.payload)
    return job.to_dict()


@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job.to_dict()
