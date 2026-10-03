import asyncio

import pytest

# Upper bound on how long a test waits for an expected event. It only fires
# when the code under test is broken, so it never slows down a passing run.
WAIT_TIMEOUT = 2.0


@pytest.fixture
def anyio_backend():
    return "asyncio"


class GatedProcessor:
    """Fake processor whose jobs block until the test releases them.

    Lets tests observe jobs mid-processing and control exactly when they
    finish, instead of relying on sleeps.
    """

    def __init__(self):
        self.started: list[str] = []
        self.release = asyncio.Event()
        self._progress = asyncio.Event()

    async def __call__(self, payload: str) -> None:
        self.started.append(payload)
        self._progress.set()
        await self.release.wait()
        if "fail" in payload:
            raise RuntimeError("internal failure detail")

    async def wait_started(self, count: int) -> None:
        async def _wait():
            while len(self.started) < count:
                self._progress.clear()
                await self._progress.wait()

        await asyncio.wait_for(_wait(), WAIT_TIMEOUT)


@pytest.fixture
def processor():
    return GatedProcessor()
