
class AsyncIterWrapper:
    """Async wrapper for sync iterables

    Copied from aitertools package.
    """

    def __init__(self, iterable):
        self._it = iter(iterable)

    async def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration as e:
            raise StopAsyncIteration() from e

    def __repr__(self):
        return '<AsyncIterWrapper {}>'.format(self._it)


async def alist(iterable):
    return [value async for value in iterable]


async def anext(iterable):
    return await iterable.__anext__()
