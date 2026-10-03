# Implementation notes

## Issues found and fixed

| Area | Problem | Fix |
|---|---|---|
| Backpressure | `create_job` awaited `queue.put()`, so a full queue blocked the request forever. | `put_nowait` → `QueueFullError` → `503` with `Retry-After`. The job is only stored once it is enqueued, so rejected jobs don't linger as `queued`. |
| Shutdown | `stop()` cancelled workers without awaiting them, never stopped new work, and left jobs `queued`/`processing` forever. | `stop()` rejects new jobs, drains for up to `shutdown_timeout`, cancels and awaits the workers, and marks unfinished jobs `failed`. Idempotent. |
| Lifecycle | The service was a module global created at import time, so its queue could belong to a different event loop than the one serving requests. `start()` twice spawned duplicate workers. | The service is created in the lifespan (`create_app()`), stored on `app.state`. `start()` is idempotent. |
| Worker | A missing job raised outside `try`, killing the worker and skipping `task_done()`. A cancelled job stayed `processing`. | Everything after `queue.get()` is inside `try/finally`; cancellation marks the job failed and re-raises. |
| HTTP | Missing/mistyped `payload` gave 422 with pydantic internals; malformed JSON likewise; unhandled errors were plain text; `str(exc)` was exposed as the job error. | All validation errors → `400 {"detail": ...}`; generic JSON `500`; job errors use a fixed message and the exception is logged. |
| Run | `python -m app.main` did nothing (no `__main__` block). | Runs uvicorn on `:8080` with a bounded graceful-shutdown timeout. |
| Tests | API tests never ran the lifespan, so no workers existed (2 tests failed); tests shared global state and polled with sleeps; `pyproject.toml` configured pytest-asyncio, which isn't installed. | Tests run the lifespan per app, use a gated fake processor to control exactly when jobs finish, and wait on `queue.join()` instead of sleeping. |

## Design decisions

- **Concurrency model.** All state is owned by one asyncio event loop. Each state transition is written with no `await` in the middle, so handlers and workers can never see a half-updated job, and a lock would add nothing. Callers get copies of jobs, never the shared objects.
- **503 for a full queue.** The server is out of capacity (not a per-client rate limit), so `503 Service Unavailable` + `Retry-After` fits better than `429`.
- **Shutdown policy.** Finish what we can within a bounded grace period, then fail the rest explicitly. A job is never left in a non-terminal state. uvicorn itself stops accepting connections before the lifespan shutdown runs.
- **Scope.** State is in-memory and per-process, so running uvicorn with `--workers > 1` would split jobs across processes. Persisting jobs would need a shared store, which is out of scope here.
