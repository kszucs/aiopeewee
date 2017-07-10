import uuid
from functools import wraps
# from peewee import ExecutionContext, Using


class _aio_callable_context_manager(object):
    __slots__ = ()

    def __call__(self, fn):
        @wraps(fn)
        async def inner(*args, **kwargs):
            async with self:
                return fn(*args, **kwargs)
        return inner


# class AioExecutionContext(_aio_callable_context_manager, ExecutionContext):

#     def __enter__(self):
#         raise NotImplementedError()

#     async def __aenter__(self):
#         async with self.database._conn_lock:
#             self.database.push_execution_context(self)
#             self.connection = await self.database._connect(
#                 self.database.database,
#                 **self.database.connect_kwargs)
#             if self.with_transaction:
#                 self.txn = self.database.transaction()
#                 await self.txn.__aenter__()
#         return self

#     def __exit__(self, exc_type, exc_val, exc_tb):
#         raise NotImplementedError()

#     async def __aexit__(self, exc_type, exc_val, exc_tb):
#         async with self.database._conn_lock:
#             if self.connection is None:
#                 self.database.pop_execution_context()
#             else:
#                 try:
#                     if self.with_transaction:
#                         if not exc_type:
#                             self.txn.commit(False)
#                         await self.txn.__aexit__(exc_type, exc_val, exc_tb)
#                 finally:
#                     self.database.pop_execution_context()
#                     await self.database._close(self.connection)


# class AioUsing(AioExecutionContext, Using):

#     def __enter__(self):
#         raise NotImplementedError()

#     async def __aenter__(self):
#         self._orig = []
#         for model in self.models:
#             self._orig.append(model._meta.database)
#             model._meta.database = self.database
#         return super(Using, self).__aenter__()

#     def __exit__(self, exc_type, exc_val, exc_tb):
#         raise NotImplementedError()

#     async def __aexit__(self, exc_type, exc_val, exc_tb):
#         await super(Using, self).__aexit__(exc_type, exc_val, exc_tb)
#         for i, model in enumerate(self.models):
#             model._meta.database = self._orig[i]


class _aio_atomic(_aio_callable_context_manager):

    __slots__ = ('conn', 'transaction_type', 'context_manager')

    def __init__(self, conn, transaction_type=None):
        self.conn = conn
        self.transaction_type = transaction_type

    async def __aenter__(self):
        await self.conn.__aenter__()
        if self.conn.transaction_depth() == 0:
            self.context_manager = self.conn.transaction(self.transaction_type)
        else:
            self.context_manager = self.conn.savepoint()
        return await self.context_manager.__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.context_manager.__aexit__(exc_type, exc_val, exc_tb)
        await self.conn.__aexit__(exc_type, exc_val, exc_tb)


class aio_transaction(_aio_callable_context_manager):

    __slots__ = ('conn', 'autocommit', 'transaction_type')

    def __init__(self, conn, transaction_type=None):
        self.conn = conn
        self.transaction_type = transaction_type

    async def _begin(self):
        if self.transaction_type:
            await self.conn.begin(self.transaction_type)
        else:
            await self.conn.begin()

    async def commit(self, begin=True):
        await self.conn.commit()
        if begin:
            await self._begin()

    async def rollback(self, begin=True):
        await self.conn.rollback()
        if begin:
            await self._begin()

    async def __aenter__(self):
        self.autocommit = self.conn.autocommit
        self.conn.autocommit = False

        if self.conn.transaction_depth() == 0:
            await self._begin()
        self.conn.push_transaction(self)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                await self.rollback(False)
            elif self.conn.transaction_depth() == 1:
                try:
                    await self.commit(False)
                except:
                    await self.rollback(False)
                    raise
        finally:
            self.conn.autocommit = self.autocommit
            self.conn.pop_transaction()


class aio_savepoint(_aio_callable_context_manager):

    __slots__ = ('conn', 'sid', 'quoted_sid', 'autocommit')

    def __init__(self, conn, sid=None):
        self.conn = conn
        self.sid = sid or uuid.uuid4().hex

        _compiler = conn.compiler()  # TODO: breing the compiler here somehow
        self.quoted_sid = _compiler.quote(self.sid)

    async def _execute(self, query):
        await self.conn.execute_sql(query, require_commit=False)

    async def _begin(self):
        await self._execute('SAVEPOINT %s;' % self.quoted_sid)

    async def commit(self, begin=True):
        await self._execute('RELEASE SAVEPOINT %s;' % self.quoted_sid)
        if begin:
            await self._begin()

    async def rollback(self):
        await self._execute('ROLLBACK TO SAVEPOINT %s;' % self.quoted_sid)

    def __enter__(self):
        raise NotImplementedError()

    async def __aenter__(self):
        self.autocommit = self.conn.get_autocommit()
        self.conn.set_autocommit(False)
        await self._begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                await self.rollback()
            else:
                try:
                    await self.commit(begin=False)
                except:
                    await self.rollback()
                    raise
        finally:
            self.conn.set_autocommit(self.autocommit)
