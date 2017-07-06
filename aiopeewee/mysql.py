import aiomysql

from peewee import mysql, ImproperlyConfigured
from peewee import (MySQLDatabase, IndexMetadata,
                    ColumnMetadata, ForeignKeyMetadata)

from .database import AioDatabase


class AioMySQLDatabase(AioDatabase, MySQLDatabase):

    async def _connect(self, database, **kwargs):
        if not mysql:
            raise ImproperlyConfigured('MySQLdb or PyMySQL must be installed.')
        conn_kwargs = {
            'charset': 'utf8',
            'use_unicode': True,
        }
        conn_kwargs.update(kwargs)
        return await aiomysql.create_pool(db=database, **conn_kwargs)

    async def get_tables(self, schema=None):
        async with self.get_conn() as conn:
            cursor = await conn.execute_sql('SHOW TABLES')
            return [row for row, in await cursor.fetchall()]

    async def get_indexes(self, table, schema=None):
        unique = set()
        indexes = {}
        async with self.get_conn() as conn:
            sql = 'SHOW INDEX FROM `%s`' % table
            cursor = await conn.execute_sql(sql)
            for row in cursor.fetchall():
                if not row[1]:
                    unique.add(row[2])
                indexes.setdefault(row[2], [])
                indexes[row[2]].append(row[4])

        return [IndexMetadata(name, None, indexes[name], name in unique, table)
                for name in indexes]

    async def get_columns(self, table, schema=None):
        sql = """
            SELECT column_name, is_nullable, data_type
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = DATABASE()"""
        async with self.get_conn() as conn:
            cursor = await conn.execute_sql(sql, (table,))
            rows = await cursor.fetchall()

        pks = set(self.get_primary_keys(table))
        return [ColumnMetadata(name, dt, null == 'YES', name in pks, table)
                for name, null, dt in rows]

    async def get_primary_keys(self, table, schema=None):
        async with self.get_conn() as conn:
            sql = 'SHOW INDEX FROM `%s`' % table
            cursor = await conn.execute_sql(sql)
            rows = await cursor.fetchall()
        return [row[4] for row in rows if row[2] == 'PRIMARY']

    async def get_foreign_keys(self, table, schema=None):
        query = """
            SELECT column_name, referenced_table_name, referenced_column_name
            FROM information_schema.key_column_usage
            WHERE table_name = %s
                AND table_schema = DATABASE()
                AND referenced_table_name IS NOT NULL
                AND referenced_column_name IS NOT NULL"""
        async with self.get_conn() as conn:
            cursor = await conn.execute_sql(query, (table,))
            rows = await cursor.fetchall()
        return [ForeignKeyMetadata(column, dest_table, dest_column, table)
                for column, dest_table, dest_column in rows]

    def get_binary_type(self):
        return mysql.Binary
