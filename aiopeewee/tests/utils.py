import logging

from peewee import logger
from contextlib import contextmanager


class QueryLogHandler(logging.Handler):

    def __init__(self, *args, **kwargs):
        self._queries = []
        super().__init__(*args, **kwargs)

    def emit(self, record):
        self._queries.append(record)

    def queries(self, ignore_txn=False):
        queries = [x.msg for x in self._queries]
        if ignore_txn:
            skips = ('BEGIN', 'ROLLBACK', 'COMMIT', 'SAVEPOINT', 'RELEASE')
            queries = [q for q in queries if not q[0].startswith(skips)]
        return queries


@contextmanager
def assert_query_count(num, ignore_txn=False):
    qh = QueryLogHandler()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(qh)
    try:
        qc0 = len(qh.queries(ignore_txn=ignore_txn))
        yield qh
    finally:
        logger.removeHandler(qh)
        qc1 = len(qh.queries(ignore_txn=ignore_txn))
        assert (qc1 - qc0) == num


def assert_queries_equal(queries, expected, db):
    queries.sort()
    expected.sort()
    for i in range(len(queries)):
        sql, params = queries[i]
        expected_sql, expected_params = expected[i]
        expected_sql = (expected_sql
                        .replace('`', db.quote_char)
                        .replace('%%', db.interpolation))
        assert sql == expected_sql
        assert params == expected_params
