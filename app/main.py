import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .processor import process_payload
from .service import (
    JobService,
    Processor,
    QueueFullError,
    ServiceNotRunningError,
)

logger = logging.getLogger(__name__)


class CreateJobRequest(BaseModel):
    payload: str


def create_app(
    worker_count: int = 2,
    queue_capacity: int = 3,
    processor: Processor = process_payload,
    shutdown_timeout: float = 5.0,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Created here, not at import time, so the service and its queue
        # belong to the event loop that actually runs the app.
        service = JobService(
            worker_count=worker_count,
            queue_capacity=queue_capacity,
            processor=processor,
            shutdown_timeout=shutdown_timeout,
        )
        await service.start()
        app.state.service = service
        try:
            yield
        finally:
            await service.stop()

    app = FastAPI(title="Job Processing Service", lifespan=lifespan)

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(request: Request, exc: RequestValidationError):
        if any(error["type"] == "json_invalid" for error in exc.errors()):
            detail = "request body must be valid JSON"
        else:
            detail = "payload is required and must be a string"
        return JSONResponse(status_code=400, content={"detail": detail})

    @app.exception_handler(Exception)
    async def handle_unexpected_error(request: Request, exc: Exception):
        logger.exception("unhandled error on %s %s", request.method, request.url.path)
        return JSONResponse(status_code=500, content={"detail": "internal server error"})

    @app.post("/jobs", status_code=201)
    async def create_job(body: CreateJobRequest, request: Request):
        if not body.payload.strip():
            raise HTTPException(status_code=400, detail="payload must not be blank")

        service: JobService = request.app.state.service
        try:
            job = await service.create_job(body.payload)
        except QueueFullError:
            raise HTTPException(
                status_code=503,
                detail="job queue is full, retry later",
                headers={"Retry-After": "1"},
            )
        except ServiceNotRunningError:
            raise HTTPException(status_code=503, detail="service is shutting down")
        return job.to_dict()

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: str, request: Request):
        service: JobService = request.app.state.service
        job = service.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job.to_dict()

    return app


app = create_app()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # uvicorn stops accepting connections on SIGINT/SIGTERM, waits (bounded) for
    # in-flight requests, then runs the lifespan shutdown that stops the workers.
    uvicorn.run(app, host="0.0.0.0", port=8080, timeout_graceful_shutdown=10)
