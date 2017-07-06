import pytest
import asyncio

from models import *

# @pytest.fixture
# async def loop(event_loop):
#     return event_loop


@pytest.yield_fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.yield_fixture(scope='session')
async def tables():
    tables = [User, Blog, EmptyModel, NoPKModel,
              Category, UserCategory, UniqueMultiField,
              NonIntModel]
    try:
        await db.connect()
        await db.create_tables(tables, safe=True)
        yield tables
    finally:
        await db.drop_tables(tables, safe=True)
        await db.close()


@pytest.fixture
async def flushdb(tables):
    for table in reversed(tables):
        await table.delete()
    return True
