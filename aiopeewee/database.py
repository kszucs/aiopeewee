import asyncio

from peewee import Database, ExceptionWrapper, basestring
from peewee import sort_models_topologically, merge_dict
from peewee import (RESULTS_NAIVE, RESULTS_TUPLES, RESULTS_DICTS,
                    RESULTS_AGGREGATE_MODELS)

from .context import _aio_atomic, aio_transaction, aio_savepoint
from .result import (AioNaiveQueryResultWrapper, AioModelQueryResultWrapper,
                     AioTuplesQueryResultWrapper, AioDictQueryResultWrapper,
                     AioAggregateQueryResultWrapper)


class AioDatabase(Database):

    def __init__(self, database, autocommit=True,
                 fields=None, ops=None, autorollback=False,
                 **connect_kwargs):
        self._closed = True
        self._conn_pool = None

        self.connect_kwargs = {}
        self.init(database, **connect_kwargs)

        self.autocommit = autocommit
        self.autorollback = autorollback
        self.use_speedups = False

        self.field_overrides = merge_dict(self.field_overrides, fields or {})
        self.op_overrides = merge_dict(self.op_overrides, ops or {})
        self.exception_wrapper = ExceptionWrapper(self.exceptions)

    def is_closed(self):
         return self._closed

    async def get_conn(self):
        if self._closed:
            with self.exception_wrapper:
                await self.connect()

        return await self._conn_pool.acquire()

    async def get_cursor(self):
        raise NotImplementedError()

    async def _close(self, conn):
        await self._conn_pool.release(conn)

    async def close(self):
        if self.deferred:
            raise Exception('Error, database not properly initialized '
                            'before closing connection')
        with self.exception_wrapper:
            if not self._closed and self._conn_pool:
                self._conn_pool.close()
                self._closed = True
                await self._conn_pool.wait_closed()

    async def connect(self, loop=None):
        if self.deferred:
            raise OperationalError('Database has not been initialized')
        if not self._closed:
            raise OperationalError('Connection already open')
        self._conn_pool = await self._create_connection(loop=loop)
        self._closed = False
        with self.exception_wrapper:
            self.initialize_connection(self._conn_pool)

    async def _create_connection(self, loop=None):
        with self.exception_wrapper:
            return await self._connect(self.database, loop=loop,
                                       **self.connect_kwargs)

    async def execute_sql(self, sql, params=None, require_commit=True):
        with self.exception_wrapper:
            conn = await self.get_conn()
            try:
                cursor = await conn.cursor()
                await cursor.execute(sql, params or ())
                # TODO: MIGHT CLOSE THE CURSOR FROM RESULT WRAPPER...
                await cursor.close()
            except Exception:
                if self.get_autocommit() and self.autorollback:
                    await conn.rollback()
                raise
            else:
                if require_commit and self.get_autocommit():
                    await conn.commit()
            finally:
                await self._close(conn)

        return cursor

    def get_result_wrapper(self, wrapper_type):
        if wrapper_type == RESULTS_NAIVE:
            return AioNaiveQueryResultWrapper
        elif wrapper_type == RESULTS_MODELS:
            return AioModelQueryResultWrapper
        elif wrapper_type == RESULTS_TUPLES:
            return AioTuplesQueryResultWrapper
        elif wrapper_type == RESULTS_DICTS:
            return AioDictQueryResultWrapper
        elif wrapper_type == RESULTS_AGGREGATE_MODELS:
            return AioAggregateQueryResultWrapper
        else:
            return AioNaiveQueryResultWrapper

    def set_autocommit(self, autocommit):
        self.autocommit = autocommit

    def get_autocommit(self):
        if self.autocommit is None:
            self.set_autocommit(self.autocommit)
        return self.autocommit

    async def commit(self):
        with self.exception_wrapper:
            await self.get_conn().commit()

    async def rollback(self):
        with self.exception_wrapper:
            await self.get_conn().rollback()

    def transaction(self, transaction_type=None):
        return aio_transaction(self, transaction_type)
    commit_on_success = property(transaction)

    def savepoint(self, sid=None):
        if not self.savepoints:
            raise NotImplementedError
        return aio_savepoint(self, sid)

    def atomic(self, transaction_type=None):
        return _aio_atomic(self, transaction_type)

    async def get_tables(self, schema=None):
        raise NotImplementedError

    async def get_indexes(self, table, schema=None):
        raise NotImplementedError

    async def get_columns(self, table, schema=None):
        raise NotImplementedError

    async def get_primary_keys(self, table, schema=None):
        raise NotImplementedError

    async def get_foreign_keys(self, table, schema=None):
        raise NotImplementedError

    async def sequence_exists(self, seq):
        raise NotImplementedError

    async def create_table(self, model_class, safe=False):
        qc = self.compiler()
        return await self.execute_sql(*qc.create_table(model_class, safe))

    async def create_tables(self, models, safe=False):
        await create_model_tables(models, fail_silently=safe)

    async def create_index(self, model_class, fields, unique=False):
        qc = self.compiler()
        if not isinstance(fields, (list, tuple)):
            raise ValueError('Fields passed to "create_index" must be a list '
                             'or tuple: "%s"' % fields)
        fobjs = [
            model_class._meta.fields[f] if isinstance(f, basestring) else f
            for f in fields]
        return await self.execute_sql(*qc.create_index(model_class, fobjs, unique))

    async def drop_index(self, model_class, fields, safe=False):
        qc = self.compiler()
        if not isinstance(fields, (list, tuple)):
            raise ValueError('Fields passed to "drop_index" must be a list '
                             'or tuple: "%s"' % fields)
        fobjs = [
            model_class._meta.fields[f] if isinstance(f, basestring) else f
            for f in fields]
        return await self.execute_sql(*qc.drop_index(model_class, fobjs, safe))

    async def create_foreign_key(self, model_class, field, constraint=None):
        qc = self.compiler()
        return await self.execute_sql(*qc.create_foreign_key(
            model_class, field, constraint))

    async def create_sequence(self, seq):
        if self.sequences:
            qc = self.compiler()
            return await self.execute_sql(*qc.create_sequence(seq))

    async def drop_table(self, model_class, fail_silently=False, cascade=False):
        qc = self.compiler()
        if cascade and not self.drop_cascade:
            raise ValueError('Database does not support DROP TABLE..CASCADE.')
        return await self.execute_sql(*qc.drop_table(
            model_class, fail_silently, cascade))

    async def drop_tables(self, models, safe=False, cascade=False):
        await drop_model_tables(models, fail_silently=safe, cascade=cascade)

    async def truncate_table(self, model_class, restart_identity=False,
                             cascade=False):
        qc = self.compiler()
        return await self.execute_sql(*qc.truncate_table(
            model_class, restart_identity, cascade))

    async def truncate_tables(self, models, restart_identity=False,
                              cascade=False):
        for model in reversed(sort_models_topologically(models)):
            await model.truncate_table(restart_identity, cascade)

    async def drop_sequence(self, seq):
        if self.sequences:
            qc = self.compiler()
            return await self.execute_sql(*qc.drop_sequence(seq))

    def extract_date(self, date_part, date_field):
        return fn.EXTRACT(Clause(date_part, R('FROM'), date_field))

    def truncate_date(self, date_part, date_field):
        return fn.DATE_TRUNC(date_part, date_field)

    def default_insert_clause(self, model_class):
        return SQL('DEFAULT VALUES')

    def get_noop_sql(self):
        return 'SELECT 0 WHERE 0'

    def get_binary_type(self):
        return binary_construct


async def create_model_tables(models, **create_table_kwargs):
    """Create tables for all given models (in the right order)."""
    for m in sort_models_topologically(models):
        await m.create_table(**create_table_kwargs)

async def drop_model_tables(models, **drop_table_kwargs):
    """Drop tables for all given models (in the right order)."""
    for m in reversed(sort_models_topologically(models)):
        await m.drop_table(**drop_table_kwargs)
