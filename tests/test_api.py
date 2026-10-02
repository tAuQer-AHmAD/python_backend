import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.fixture
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client


@pytest.mark.anyio
async def test_create_job_returns_201(client):
    response = await client.post("/jobs", json={"payload": "hello"})

    assert response.status_code == 201
    body = response.json()
    assert body["id"]
    assert body["payload"] == "hello"
    assert body["status"] == "queued"


@pytest.mark.anyio
async def test_blank_payload_is_rejected(client):
    response = await client.post("/jobs", json={"payload": "   "})

    assert response.status_code == 400


@pytest.mark.anyio
async def test_unknown_job_returns_404(client):
    response = await client.get("/jobs/does-not-exist")

    assert response.status_code == 404


@pytest.mark.anyio
async def test_job_eventually_completes(client):
    response = await client.post("/jobs", json={"payload": "hello"})
    job_id = response.json()["id"]

    for _ in range(50):
        response = await client.get(f"/jobs/{job_id}")
        if response.json()["status"] == "completed":
            break
        await __import__("asyncio").sleep(0.01)

    assert response.status_code == 200
    assert response.json()["status"] == "completed"


@pytest.mark.anyio
async def test_failed_payload_is_reported(client):
    response = await client.post("/jobs", json={"payload": "please fail"})
    job_id = response.json()["id"]

    for _ in range(50):
        response = await client.get(f"/jobs/{job_id}")
        if response.json()["status"] == "failed":
            break
        await __import__("asyncio").sleep(0.01)

    body = response.json()
    assert body["status"] == "failed"
    assert body["error"]
