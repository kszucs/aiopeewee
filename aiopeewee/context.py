from peewee import ExecutionContext, Using, _atomic, transaction, savepoint


class _aio_callable_context_manager(object):
    __slots__ = ()
    def __call__(self, fn):
        @wraps(fn)
        async def inner(*args, **kwargs):
            async with self:
                return fn(*args, **kwargs)
        return inner


class AioExecutionContext(_aio_callable_context_manager, ExecutionContext):

    async def __aenter__(self):
        async with self.database._conn_lock:
            self.database.push_execution_context(self)
            self.connection = await self.database._connect(
                self.database.database,
                **self.database.connect_kwargs)
            if self.with_transaction:
                self.txn = self.database.transaction()
                await self.txn.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        async with self.database._conn_lock:
            if self.connection is None:
                self.database.pop_execution_context()
            else:
                try:
                    if self.with_transaction:
                        if not exc_type:
                            self.txn.commit(False)
                        await self.txn.__aexit__(exc_type, exc_val, exc_tb)
                finally:
                    self.database.pop_execution_context()
                    await self.database._close(self.connection)


class AioUsing(AioExecutionContext, Using):

    async def __aenter__(self):
        self._orig = []
        for model in self.models:
            self._orig.append(model._meta.database)
            model._meta.database = self.database
        return await super(Using, self).__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await super(Using, self).__aexit__(exc_type, exc_val, exc_tb)
        for i, model in enumerate(self.models):
            model._meta.database = self._orig[i]


class _aio_atomic(_aio_callable_context_manager, _atomic):

    async def __aenter__(self):
        if self.db.transaction_depth() == 0:
            self.context_manager = self.db.transaction(self.transaction_type)
        else:
            self.context_manager = self.db.savepoint()
        return await self.context_manager.__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.context_manager.__aexit__(exc_type, exc_val, exc_tb)


class aio_transaction(_aio_callable_context_manager, transaction):

    async def _begin(self):
        if self.transaction_type:
            await self.db.begin(self.transaction_type)
        else:
            await self.db.begin()

    async def commit(self, begin=True):
        await self.db.commit()
        if begin: await self._begin()

    async def rollback(self, begin=True):
        await self.db.rollback()
        if begin: await self._begin()

    async def __aenter__(self):
        self.autocommit = self.db.get_autocommit()
        self.db.set_autocommit(False)
        if self.db.transaction_depth() == 0: await self._begin()
        self.db.push_transaction(self)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                await self.rollback(False)
            elif self.db.transaction_depth() == 1:
                try:
                    await self.commit(False)
                except:
                    await self.rollback(False)
                    raise
        finally:
            self.db.set_autocommit(self.autocommit)
            self.db.pop_transaction()


class aio_savepoint(_aio_callable_context_manager, savepoint):

    async def _execute(self, query):
        await self.db.execute_sql(query, require_commit=False)

    async def _begin(self):
        await self._execute('SAVEPOINT %s;' % self.quoted_sid)

    async def commit(self, begin=True):
        await self._execute('RELEASE SAVEPOINT %s;' % self.quoted_sid)
        if begin: await self._begin()

    async def rollback(self):
        await self._execute('ROLLBACK TO SAVEPOINT %s;' % self.quoted_sid)

    async def __aenter__(self):
        self.autocommit = self.db.get_autocommit()
        self.db.set_autocommit(False)
        await self._begin()
        return self

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
            self.db.set_autocommit(self.autocommit)
