# Python Backend Engineer Skill Test — Job Processing Service

## Overview

You are given a small Python HTTP service that accepts jobs and processes them asynchronously using a worker pool.

The repository intentionally contains a few implementation and reliability issues. Your task is to identify and fix them while keeping the existing API contract.

**Expected time:** 1–2 hours.

## Goal

Make the service production-ready for the scenarios covered by the existing tests and the requirements below.

## API

### `POST /jobs`

Creates a job.

Request:

```json
{"payload":"hello"}
```

Requirements:

- `payload` is required and must not be blank.
- Return `201 Created` with the created job as JSON.
- Each job has a unique ID and starts with status `queued`.

### `GET /jobs/{id}`

Returns a job by ID.

Requirements:

- Return `200 OK` when found.
- Return `404 Not Found` when the ID does not exist.
- A job moves through `queued` → `processing` → `completed`.
- If processing fails, the job must become `failed` and expose an error message.

## Requirements

### 1. Concurrency safety

- The service must be safe under concurrent HTTP requests and workers.
- Concurrent test execution must not produce data races or inconsistent job state.
- Shared state must be protected appropriately.

### 2. Worker pool

- The configured number of workers should process queued jobs concurrently.
- Do not create an unbounded background task per job.
- Worker lifecycle must be explicit and testable.

### 3. Backpressure

- The queue has a finite capacity.
- When the queue is full, `POST /jobs` must fail cleanly rather than waiting forever.
- The API should return an appropriate HTTP error response.

### 4. Context and shutdown

- The server should stop accepting new work during shutdown.
- Workers must exit cleanly when the service is stopped.
- Do not leave background tasks running indefinitely.
- Shutdown should be bounded and predictable.

### 5. HTTP behavior

- Return appropriate status codes and JSON responses.
- Do not expose internal implementation details.
- Do not crash on malformed JSON or invalid requests.
- Error responses should be valid JSON.

### 6. Testing

- Fix or add tests where appropriate.
- At minimum, cover important concurrency, queue-full, shutdown, validation, and processing-error paths you change.
- Tests should be deterministic and should not depend on arbitrary long sleeps.

### 7. Code quality

- Keep the public API and project structure reasonably close to the provided version.
- Prefer simple, idiomatic Python over unnecessary abstractions.
- Avoid introducing unnecessary frameworks or dependencies.

## Processing behavior

The supplied processor simulates work.

- Payloads containing the string `fail` should result in a failed job.
- Other payloads should complete successfully.

You do **not** need to add authentication, a database, Docker, external services, or any blockchain/Web3 functionality.

## Environment

- Python 3.10+
- Linux/macOS/Windows
- Git

## How to run

Create a virtual environment and install the requirements:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run the tests:

```bash
pytest
```

Start the server:

```bash
python -m app.main
```

The server listens on `:8080` by default.

## Git workflow — required

The repository includes its `.git` directory. Please preserve the existing Git history.

Before starting the task:

```bash
git status
git switch -c candidate/<your-name>
git add .
git commit -m "start test"
```

Then make your changes on `candidate/<your-name>`.

When you finish:

```bash
git add .
git commit -m "done test"
git status
```

Please submit the repository with the `.git` directory included so we can review your implementation and commit history.

## Evaluation

We will primarily evaluate:

- correctness and API behavior
- concurrency safety and state consistency
- worker-pool behavior
- backpressure
- graceful shutdown and resource management
- quality and determinism of tests
- idiomatic Python and maintainability
- ability to reason about failure cases
- Git workflow and clarity of commits

Please do not spend time on cosmetic changes. Focus on correctness, reliability, and clear engineering decisions.
