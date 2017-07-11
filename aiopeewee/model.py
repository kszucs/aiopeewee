from peewee import Model, ModelAlias, IntegrityError

from .query import (AioSelectQuery, AioUpdateQuery, AioInsertQuery,
                    AioDeleteQuery, AioRawQuery, AioNoopSelectQuery)


class AioModelAlias(ModelAlias):

    def select(self, *selection):
        if not selection:
            selection = self.get_proxy_fields()
        query = AioSelectQuery(self, *selection)
        if self._meta.order_by:
            query = query.order_by(*self._meta.order_by)
        return query

    def __iter__(self):
        raise NotImplementedError()


class AioModel(Model):

    def __iter__(self):
        raise NotImplementedError()

    @classmethod
    def alias(cls):
        return AioModelAlias(cls)

    @classmethod
    def select(cls, *selection):
        query = AioSelectQuery(cls, *selection)
        if cls._meta.order_by:
            query = query.order_by(*cls._meta.order_by)
        return query

    @classmethod
    def update(cls, __data=None, **update):
        fdict = __data or {}
        fdict.update([(cls._meta.fields[f], update[f]) for f in update])
        return AioUpdateQuery(cls, fdict)

    @classmethod
    def insert(cls, __data=None, **insert):
        fdict = __data or {}
        fdict.update([(cls._meta.fields[f], insert[f]) for f in insert])
        return AioInsertQuery(cls, fdict)

    @classmethod
    def insert_many(cls, rows, validate_fields=True):
        return AioInsertQuery(cls, rows=rows, validate_fields=validate_fields)

    @classmethod
    def insert_from(cls, fields, query):
        return AioInsertQuery(cls, fields=fields, query=query)

    @classmethod
    def delete(cls):
        return AioDeleteQuery(cls)

    @classmethod
    def raw(cls, sql, *params):
        return AioRawQuery(cls, sql, *params)

    @classmethod
    async def create(cls, **query):
        inst = cls(**query)
        await inst.save(force_insert=True)
        inst._prepare_instance()
        return inst

    @classmethod
    async def get(cls, *query, **kwargs):
        sq = cls.select().naive()
        if query:
            sq = sq.where(*query)
        if kwargs:
            sq = sq.filter(**kwargs)
        return await sq.get()

    @classmethod
    async def get_or_create(cls, **kwargs):
        defaults = kwargs.pop('defaults', {})
        query = cls.select()
        for field, value in kwargs.items():
            if '__' in field:
                query = query.filter(**{field: value})
            else:
                query = query.where(getattr(cls, field) == value)

        try:
            return await query.get(), False
        except cls.DoesNotExist:
            try:
                params = dict((k, v) for k, v in kwargs.items()
                              if '__' not in k)
                params.update(defaults)

                async with cls._meta.database.atomic():
                    return await cls.create(**params), True
            except IntegrityError as exc:
                try:
                    return await query.get(), False
                except cls.DoesNotExist:
                    raise exc

    @classmethod
    async def table_exists(cls):
        kwargs = {}
        if cls._meta.schema:
            kwargs['schema'] = cls._meta.schema
        tables = await cls._meta.database.get_tables(**kwargs)
        return cls._meta.db_table in tables

    @classmethod
    async def create_table(cls, fail_silently=False):
        if fail_silently and await cls.table_exists():
            return

        db = cls._meta.database
        pk = cls._meta.primary_key
        if db.sequences and pk is not False and pk.sequence:
            if not db.sequence_exists(pk.sequence):
                db.create_sequence(pk.sequence)

        await db.create_table(cls)
        await cls._create_indexes()

    @classmethod
    async def _create_indexes(cls):
        for field_list, is_unique in cls._index_data():
            await cls._meta.database.create_index(cls, field_list, is_unique)

    @classmethod
    async def _drop_indexes(cls, safe=False):
        for field_list, is_unique in cls._index_data():
            await cls._meta.database.drop_index(cls, field_list, safe)

    @classmethod
    async def drop_table(cls, fail_silently=False, cascade=False):
        await cls._meta.database.drop_table(cls, fail_silently, cascade)

    @classmethod
    async def truncate_table(cls, restart_identity=False, cascade=False):
        await cls._meta.database.truncate_table(cls, restart_identity, cascade)

    @classmethod
    def noop(cls, *args, **kwargs):
        return AioNoopSelectQuery(cls, *args, **kwargs)

    async def save(self, force_insert=False, only=None):
        field_dict = dict(self._data)
        if self._meta.primary_key is not False:
            pk_field = self._meta.primary_key
            pk_value = self._get_pk_value()
        else:
            pk_field = pk_value = None
        if only:
            field_dict = self._prune_fields(field_dict, only)
        elif self._meta.only_save_dirty and not force_insert:
            field_dict = self._prune_fields(
                field_dict,
                self.dirty_fields)
            if not field_dict:
                self._dirty.clear()
                return False

        self._populate_unsaved_relations(field_dict)
        if pk_value is not None and not force_insert:
            if self._meta.composite_key:
                for pk_part_name in pk_field.field_names:
                    field_dict.pop(pk_part_name, None)
            else:
                field_dict.pop(pk_field.name, None)
            rows = await (self.update(**field_dict)
                              .where(self._pk_expr())
                              .execute())
        elif pk_field is None:
            await self.insert(**field_dict).execute()
            rows = 1
        else:
            pk_from_cursor = await self.insert(**field_dict).execute()
            if pk_from_cursor is not None:
                pk_value = pk_from_cursor
            self._set_pk_value(pk_value)
            rows = 1
        self._dirty.clear()
        return rows

    async def delete_instance(self, recursive=False, delete_nullable=False):
        if recursive:
            dependencies = self.dependencies(delete_nullable)
            for query, fk in reversed(list(dependencies)):
                model = fk.model_class
                if fk.null and not delete_nullable:
                    await (model.update(**{fk.name: None})
                                .where(query)
                                .execute())
                else:
                    await model.delete().where(query).execute()
        return await self.delete().where(self._pk_expr()).execute()
