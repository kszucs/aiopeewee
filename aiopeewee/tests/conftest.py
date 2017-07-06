import pytest


@pytest.fixture
async def loop(event_loop):
    return event_loop
