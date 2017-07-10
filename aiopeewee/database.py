import threading
from peewee import Database, ExceptionWrapper, basestring
from peewee import sort_models_topologically, merge_dict
from peewee import OperationalError
from peewee import (RESULTS_NAIVE, RESULTS_TUPLES, RESULTS_DICTS,
                    RESULTS_AGGREGATE_MODELS, RESULTS_MODELS)
from peewee import SQL, R, Clause, fn, binary_construct
from peewee import logger

from .context import _aio_atomic, aio_transaction, aio_savepoint
from .result import (AioNaiveQueryResultWrapper, AioModelQueryResultWrapper,
                     AioTuplesQueryResultWrapper, AioDictQueryResultWrapper,
                     AioAggregateQueryResultWrapper)


# remove this one, just use autocommit arg in db.execute_sql
# in case of a transaction, the connection should be bounded
# to the atomic/transaction context manager
class AioConnection(object):

    def __init__(self, acquirer, exception_wrapper,
                 autocommit=None, autorollback=None):
        self.autocommit = autocommit
        self.autorollback = autorollback
        self.acquirer = acquirer
        self.closed = True
        self.conn = None
        self.context_stack = []
        self.transactions = []
        self.exception_wrapper = exception_wrapper  # TODO: remove

    def transaction_depth(self):
        return len(self.transactions)

    def push_transaction(self, transaction):
        self.transactions.append(transaction)

    def pop_transaction(self):
        return self.transactions.pop()

    async def execute_sql(self, sql, params=None, require_commit=True):
        logger.debug((sql, params))
        with self.exception_wrapper:
            cursor = await self.conn.cursor()
            try:
                await cursor.execute(sql, params or ())
            except Exception:
                if self.autorollback and self.autocommit:
                    await self.rollback()
                raise
            else:
                if require_commit and self.autocommit:
                    await self.commit()
            return cursor

    async def __aenter__(self):
        self.conn = await self.acquirer.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.acquirer.__aexit__(exc_type, exc_val, exc_tb)

    async def begin(self):
        pass

    def commit(self):
        with self.exception_wrapper:
            return self.conn.commit()

    def rollback(self):
        with self.exception_wrapper:
            return self.conn.rollback()

    # def close(self):
    #     # self.conn_pool.release(conn)
    #     return self.conn.close()

    def transaction(self, transaction_type=None):
        return aio_transaction(self, transaction_type)
    commit_on_success = property(transaction)

    def savepoint(self, sid=None):
        if not self.savepoints:
            raise NotImplementedError
        return aio_savepoint(self, sid)


class AioDatabase(Database):

    def begin(self):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def rollback(self):
        raise NotImplementedError

    def get_cursor(self):
        raise NotImplementedError

    def get_tables(self, schema=None):
        raise NotImplementedError

    def get_indexes(self, table, schema=None):
        raise NotImplementedError

    def get_columns(self, table, schema=None):
        raise NotImplementedError

    def get_primary_keys(self, table, schema=None):
        raise NotImplementedError

    def get_foreign_keys(self, table, schema=None):
        raise NotImplementedError

    def sequence_exists(self, seq):
        raise NotImplementedError

    def transaction_depth(self):
        raise NotImplementedError

    def __init__(self, database, threadlocals=True, autocommit=True,
                 fields=None, ops=None, autorollback=False,
                 **connect_kwargs):
        self.connect_kwargs = {}
        self.closed = True
        self.init(database, **connect_kwargs)

        self.pool = None

        self.autocommit = autocommit
        self.autorollback = autorollback
        self.use_speedups = False

        self.field_overrides = merge_dict(self.field_overrides, fields or {})
        self.op_overrides = merge_dict(self.op_overrides, ops or {})
        self.exception_wrapper = ExceptionWrapper(self.exceptions)

    def is_closed(self):
        return self.closed

    def get_conn(self):
        if self.closed:
            raise OperationalError('Database pool has not been initialized')

        return AioConnection(self.pool.acquire(),
                             autocommit=self.autocommit,
                             autorollback=self.autorollback,
                             exception_wrapper=self.exception_wrapper)

    async def close(self):
        if self.deferred:
            raise Exception('Error, database not properly initialized '
                            'before closing connection')
        with self.exception_wrapper:
            if not self.closed and self.pool:
                self.pool.close()
                self.closed = True
                await self.pool.wait_closed()

    async def connect(self, safe=True):
        if self.deferred:
            raise OperationalError('Database has not been initialized')
        if not self.closed:
            if safe:
                return
            raise OperationalError('Connection already open')

        with self.exception_wrapper:
            self.pool = await self._connect(self.database,
                                            **self.connect_kwargs)
            self.closed = False

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

    def atomic(self, transaction_type=None):
        return _aio_atomic(self.get_conn(), transaction_type)

    def transaction(self, transaction_type=None):
        return aio_transaction(self, transaction_type)
    commit_on_success = property(transaction)

    # def savepoint(self, sid=None):
    #     if not self.savepoints:
    #         raise NotImplementedError
    #     return aio_savepoint(self, sid)

    async def create_table(self, model_class, safe=False):
        qc = self.compiler()
        async with self.get_conn() as conn:
            args = qc.create_table(model_class, safe)
            return await conn.execute_sql(*args)

    async def create_tables(self, models, safe=False):
        await create_model_tables(models, fail_silently=safe)

    async def create_index(self, model_class, fields, unique=False):
        qc = self.compiler()
        if not isinstance(fields, (list, tuple)):
            raise ValueError('Fields passed to "create_index" must be a list '
                             'or tuple: "%s"' % fields)
        fobjs = [model_class._meta.fields[f]
                 if isinstance(f, basestring) else f
                 for f in fields]
        async with self.get_conn() as conn:
            args = qc.create_index(model_class, fobjs, unique)
            return await conn.execute_sql(*args)

    async def drop_index(self, model_class, fields, safe=False):
        qc = self.compiler()
        if not isinstance(fields, (list, tuple)):
            raise ValueError('Fields passed to "drop_index" must be a list '
                             'or tuple: "%s"' % fields)
        fobjs = [model_class._meta.fields[f]
                 if isinstance(f, basestring) else f
                 for f in fields]
        async with self.get_conn() as conn:
            args = qc.drop_index(model_class, fobjs, safe)
            return await conn.execute_sql(*args)

    async def create_foreign_key(self, model_class, field, constraint=None):
        qc = self.compiler()
        async with self.get_conn() as conn:
            args = qc.create_foreign_key(model_class, field, constraint)
            return await conn.execute_sql(*args)

    async def create_sequence(self, seq):
        if self.sequences:
            qc = self.compiler()
            async with self.get_conn() as conn:
                return await conn.execute_sql(*qc.create_sequence(seq))

    async def drop_table(self, model_class, fail_silently=False, cascade=False):
        qc = self.compiler()
        if cascade and not self.drop_cascade:
            raise ValueError('Database does not support DROP TABLE..CASCADE.')
        async with self.get_conn() as conn:
            args = qc.drop_table(model_class, fail_silently, cascade)
            return await conn.execute_sql(*args)

    async def drop_tables(self, models, safe=False, cascade=False):
        await drop_model_tables(models, fail_silently=safe, cascade=cascade)

    async def truncate_table(self, model_class, restart_identity=False,
                             cascade=False):
        qc = self.compiler()
        async with self.get_conn() as conn:
            args = qc.truncate_table(model_class, restart_identity, cascade)
            return await conn.execute_sql(*args)

    async def truncate_tables(self, models, restart_identity=False,
                              cascade=False):
        for model in reversed(sort_models_topologically(models)):
            await model.truncate_table(restart_identity, cascade)

    async def drop_sequence(self, seq):
        if self.sequences:
            qc = self.compiler()
            async with self.get_conn() as conn:
                return await conn.execute_sql(*qc.drop_sequence(seq))

    async def execute_sql(self, sql, params=None, require_commit=True):
        async with self.get_conn() as conn:
            return await conn.execute_sql(sql, params,
                                          require_commit=require_commit)

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
