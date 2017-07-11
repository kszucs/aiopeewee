import operator
from peewee import SQL, Query, RawQuery, SelectQuery, NoopSelectQuery
from peewee import CompoundSelect, DeleteQuery, UpdateQuery, InsertQuery
from peewee import _WriteQuery
from peewee import RESULTS_TUPLES, RESULTS_DICTS, RESULTS_NAIVE


from aitertools import aiter, alist


class AioQuery(Query):

    async def execute(self):
        raise NotImplementedError

    async def _execute(self):
        sql, params = self.sql()
        async with self.database.get_conn() as conn:
            return await conn.execute_sql(sql, params, self.require_commit)

    async def scalar(self, as_tuple=False, convert=False):
        if convert:
            row = await self.tuples().first()
        else:
            cursor = await self._execute()
            row = await cursor.fetchone()
        if row and not as_tuple:
            return row[0]
        else:
            return row

    def __await__(self):
        return alist(self).__await__()

    def __iter__(self):
        raise NotImplementedError()

    # TODO: wath out for PEP492
    async def __aiter__(self):
        qr = await self.execute()
        return await qr.__aiter__()


class AioRawQuery(AioQuery, RawQuery):

    def clone(self):
        query = AioRawQuery(self.model_class, self._sql, *self._params)
        query._tuples = self._tuples
        query._dicts = self._dicts
        return query

    async def execute(self):
        if self._qr is None:
            if self._tuples:
                QRW = self.database.get_result_wrapper(RESULTS_TUPLES)
            elif self._dicts:
                QRW = self.database.get_result_wrapper(RESULTS_DICTS)
            else:
                QRW = self.database.get_result_wrapper(RESULTS_NAIVE)
            self._qr = QRW(self.model_class, await self._execute(), None)
        return self._qr


class AioSelectQuery(AioQuery, SelectQuery):

    def compound_op(operator):
        def inner(self, other):
            supported_ops = self.model_class._meta.database.compound_operations
            if operator not in supported_ops:
                raise ValueError(
                    'Your database does not support %s' % operator)
            return AioCompoundSelect(self.model_class, self, operator, other)
        return inner

    async def aggregate(self, aggregation=None, convert=True):
        return await self._aggregate(aggregation).scalar(convert=convert)

    async def count(self, clear_limit=False):
        if self._distinct or self._group_by or self._limit or self._offset:
            return await self.wrapped_count(clear_limit=clear_limit)

        # defaults to a count() of the primary key
        return await self.aggregate(convert=False) or 0

    async def wrapped_count(self, clear_limit=False):
        clone = self.order_by()
        if clear_limit:
            clone._limit = clone._offset = None

        sql, params = clone.sql()
        wrapped = 'SELECT COUNT(1) FROM (%s) AS wrapped_select' % sql
        rq = self.model_class.raw(wrapped, *params)
        return await rq.scalar() or 0

    async def exists(self):
        clone = self.paginate(1, 1)
        clone._select = [SQL('1')]
        return bool(await clone.scalar())

    async def get(self):
        clone = self.paginate(1, 1)
        try:
            qr = await clone.execute()
            return await qr.__anext__()
        except StopAsyncIteration:
            raise self.model_class.DoesNotExist(
                'Instance matching query does not exist:\nSQL: %s\nPARAMS: %s'
                % self.sql())

    async def peek(self, n=1):
        res = await self.execute()
        await res.fill_cache(n)
        models = res._result_cache[:n]
        if models:
            return models[0] if n == 1 else models

    async def first(self, n=1):
        if self._limit != n:
            self._limit = n
            self._dirty = True
        return await self.peek(n=n)

    def sql(self):
        return self.compiler().generate_select(self)

    async def execute(self):
        if self._dirty or self._qr is None:
            model_class = self.model_class
            query_meta = self.get_query_meta()
            ResultWrapper = self._get_result_wrapper()
            cursor = await self._execute()
            self._qr = ResultWrapper(model_class, cursor, query_meta)
            self._dirty = False
            return self._qr
        else:
            return self._qr

    async def iterator(self):
        qr = await self.execute()
        async for row in qr.iterator():
            yield row

    def __getitem__(self, value):
        raise NotImplementedError()

    def __len__(self):
        raise NotImplementedError()

    def __hash__(self):
        return id(self)


class AioNoopSelectQuery(AioSelectQuery, NoopSelectQuery):
    pass


class AioCompoundSelect(AioSelectQuery, CompoundSelect):
    _node_type = 'compound_select_query'

    async def count(self, clear_limit=False):
        return await self.wrapped_count(clear_limit=clear_limit)


class _AioWriteQuery(AioQuery, _WriteQuery):

    async def _execute_with_result_wrapper(self):
        ResultWrapper = self.get_result_wrapper()
        meta = (self._returning, {self.model_class: []})
        self._qr = ResultWrapper(self.model_class, await self._execute(), meta)
        return self._qr

    def __await__(self):
        return self.execute().__await__()


class AioUpdateQuery(_AioWriteQuery, UpdateQuery):

    async def execute(self):
        if self._returning is not None and self._qr is None:
            return await self._execute_with_result_wrapper()
        elif self._qr is not None:
            return self._qr
        else:
            return self.database.rows_affected(await self._execute())

    def __aiter__(self):
        if not self.model_class._meta.database.returning_clause:
            raise ValueError('UPDATE queries cannot be iterated over unless '
                             'they specify a RETURNING clause, which is not '
                             'supported by your database.')
        return self.execute()


class AioInsertQuery(_AioWriteQuery, InsertQuery):

    async def _insert_with_loop(self):
        id_list = []
        last_id = None
        return_id_list = self._return_id_list
        for row in self._rows:
            last_id = await (AioInsertQuery(self.model_class, row)
                             .upsert(self._upsert)
                             .execute())
            if return_id_list:
                id_list.append(last_id)

        if return_id_list:
            return id_list
        else:
            return last_id

    async def execute(self):
        insert_with_loop = (
            self._is_multi_row_insert and
            self._query is None and
            self._returning is None and
            not self.database.insert_many)
        if insert_with_loop:
            return await self._insert_with_loop()

        if self._returning is not None and self._qr is None:
            return await self._execute_with_result_wrapper()
        elif self._qr is not None:
            return self._qr
        else:
            cursor = await self._execute()
            if not self._is_multi_row_insert:
                if self.database.insert_returning:
                    pk_row = await cursor.fetchone()
                    meta = self.model_class._meta
                    clean_data = [
                        field.python_value(column)
                        for field, column
                        in zip(meta.get_primary_key_fields(), pk_row)]
                    if self.model_class._meta.composite_key:
                        return clean_data
                    return clean_data[0]
                return self.database.last_insert_id(cursor, self.model_class)
            elif self._return_id_list:
                return map(operator.itemgetter(0), await cursor.fetchall())
            else:
                return True


class AioDeleteQuery(_AioWriteQuery, DeleteQuery):

    async def execute(self):
        if self._returning is not None and self._qr is None:
            return await self._execute_with_result_wrapper()
        elif self._qr is not None:
            return self._qr
        else:
            return self.database.rows_affected(await self._execute())
