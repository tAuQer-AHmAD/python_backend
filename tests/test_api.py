import asyncio
from contextlib import asynccontextmanager

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app as default_app
from app.main import create_app

from .conftest import WAIT_TIMEOUT

pytestmark = pytest.mark.anyio


@asynccontextmanager
async def running(app, raise_app_exceptions=True):
    """Run the app's lifespan (which ASGITransport skips) around a client."""
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app, raise_app_exceptions=raise_app_exceptions)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@pytest.fixture
async def client():
    async with running(default_app) as client:
        yield client


@pytest.fixture
def gated_app(processor):
    return create_app(worker_count=2, queue_capacity=2, processor=processor)


async def wait_idle(app) -> None:
    await asyncio.wait_for(app.state.service.queue.join(), WAIT_TIMEOUT)


async def test_create_job_returns_201(client):
    response = await client.post("/jobs", json={"payload": "hello"})

    assert response.status_code == 201
    body = response.json()
    assert body["id"]
    assert body["payload"] == "hello"
    assert body["status"] == "queued"
    assert body["error"] is None


async def test_job_ids_are_unique(client):
    first = await client.post("/jobs", json={"payload": "a"})
    second = await client.post("/jobs", json={"payload": "b"})

    assert first.json()["id"] != second.json()["id"]


@pytest.mark.parametrize("payload", ["", "   ", "\n\t"])
async def test_blank_payload_is_rejected(client, payload):
    response = await client.post("/jobs", json={"payload": payload})

    assert response.status_code == 400
    assert response.json() == {"detail": "payload must not be blank"}


@pytest.mark.parametrize(
    "body",
    [{}, {"payload": None}, {"payload": 123}, {"other": "x"}, ["hello"], "hello"],
)
async def test_invalid_body_is_rejected_with_json_error(client, body):
    response = await client.post("/jobs", json=body)

    assert response.status_code == 400
    assert response.json() == {"detail": "payload is required and must be a string"}


async def test_malformed_json_is_rejected_with_json_error(client):
    response = await client.post(
        "/jobs", content=b'{"payload": ', headers={"Content-Type": "application/json"}
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "request body must be valid JSON"}


async def test_unknown_job_returns_404(client):
    response = await client.get("/jobs/does-not-exist")

    assert response.status_code == 404
    assert response.json() == {"detail": "job not found"}


async def test_job_eventually_completes(client):
    response = await client.post("/jobs", json={"payload": "hello"})
    job_id = response.json()["id"]

    await wait_idle(default_app)

    response = await client.get(f"/jobs/{job_id}")
    assert response.status_code == 200
    assert response.json()["status"] == "completed"


async def test_failed_payload_is_reported(client):
    response = await client.post("/jobs", json={"payload": "please fail"})
    job_id = response.json()["id"]

    await wait_idle(default_app)

    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "failed"
    assert body["error"]


async def test_job_status_transitions(gated_app, processor):
    async with running(gated_app) as client:
        job_id = (await client.post("/jobs", json={"payload": "hello"})).json()["id"]

        await processor.wait_started(1)
        assert (await client.get(f"/jobs/{job_id}")).json()["status"] == "processing"

        processor.release.set()
        await wait_idle(gated_app)
        assert (await client.get(f"/jobs/{job_id}")).json()["status"] == "completed"


async def test_processing_error_does_not_leak_internals(gated_app, processor):
    async with running(gated_app) as client:
        job_id = (await client.post("/jobs", json={"payload": "fail"})).json()["id"]
        processor.release.set()
        await wait_idle(gated_app)

        body = (await client.get(f"/jobs/{job_id}")).json()
        assert body["status"] == "failed"
        assert body["error"] == "processing failed"
        assert "internal failure detail" not in str(body)


async def test_full_queue_returns_503(gated_app, processor):
    async with running(gated_app) as client:
        # 2 workers busy + 2 queued fills worker_count=2, queue_capacity=2.
        for i in range(2):
            assert (await client.post("/jobs", json={"payload": f"busy-{i}"})).status_code == 201
        await processor.wait_started(2)
        for i in range(2):
            assert (await client.post("/jobs", json={"payload": f"queued-{i}"})).status_code == 201

        response = await asyncio.wait_for(
            client.post("/jobs", json={"payload": "overflow"}), WAIT_TIMEOUT
        )

        assert response.status_code == 503
        assert response.json() == {"detail": "job queue is full, retry later"}
        assert response.headers["Retry-After"] == "1"
        processor.release.set()


async def test_concurrent_requests_are_consistent(processor):
    app = create_app(worker_count=4, queue_capacity=50, processor=processor)
    async with running(app) as client:
        responses = await asyncio.gather(
            *(client.post("/jobs", json={"payload": f"job-{i}"}) for i in range(50))
        )
        assert all(r.status_code == 201 for r in responses)
        ids = {r.json()["id"] for r in responses}
        assert len(ids) == 50

        processor.release.set()
        await wait_idle(app)
        bodies = await asyncio.gather(*(client.get(f"/jobs/{job_id}") for job_id in ids))
        assert all(b.json()["status"] == "completed" for b in bodies)


async def test_post_during_shutdown_returns_503(gated_app):
    async with running(gated_app) as client:
        await gated_app.state.service.stop()

        response = await client.post("/jobs", json={"payload": "late"})

        assert response.status_code == 503
        assert response.json() == {"detail": "service is shutting down"}


async def test_unexpected_error_returns_generic_json_500(gated_app, monkeypatch):
    async with running(gated_app, raise_app_exceptions=False) as client:

        def broken(job_id):
            raise RuntimeError("secret internal state")

        monkeypatch.setattr(gated_app.state.service, "get_job", broken)

        response = await client.get("/jobs/anything")

        assert response.status_code == 500
        assert response.json() == {"detail": "internal server error"}


async def test_lifespan_shutdown_stops_workers(gated_app, processor):
    async with running(gated_app) as client:
        await client.post("/jobs", json={"payload": "hello"})
        processor.release.set()
        workers = list(gated_app.state.service.workers)

    assert workers and all(worker.done() for worker in workers)
