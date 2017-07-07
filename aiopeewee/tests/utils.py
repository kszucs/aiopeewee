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
        yield qc0
    finally:
        logger.removeHandler(qh)
        qc1 = len(qh.queries(ignore_txn=ignore_txn))
        print(qc1)
        assert (qc1 - qc0) == num
