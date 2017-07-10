from aitertools import aiter
from collections import OrderedDict

from peewee import QueryResultWrapper, ExtQueryResultWrapper
from peewee import TuplesQueryResultWrapper, DictQueryResultWrapper
from peewee import ModelQueryResultWrapper, AggregateQueryResultWrapper
from peewee import NaiveQueryResultWrapper


class AioResultIterator(object):

    def __init__(self, qrw):
        self.qrw = qrw
        self._idx = 0

    async def __anext__(self):
        if self._idx < self.qrw._ct:
            obj = self.qrw._result_cache[self._idx]
        elif not self.qrw._populated:
            obj = await self.qrw.iterate()
            self.qrw._result_cache.append(obj)
            self.qrw._ct += 1
        else:
            raise StopAsyncIteration
        self._idx += 1
        return obj


class AioQueryResultWrapper(QueryResultWrapper):

    async def __aiter__(self):
        if self._populated:
            return await aiter(self._result_cache)
        else:
            return AioResultIterator(self)

    def __await__(self):
        # TODO: fill_cache with cursor.fetchall
        return alist(self).__await__()

    async def count(self):
        await self.fill_cache()
        return self._ct

    def __len__(self):
        raise NotImplementedError()

    async def iterate(self):
        row = await self.cursor.fetchone()
        if not row:
            self._populated = True
            if not getattr(self.cursor, 'name', None):
                await self.cursor.close()
            raise StopAsyncIteration
        elif not self._initialized:
            self.initialize(self.cursor.description)
            self._initialized = True
        return self.process_row(row)

    async def iterator(self):
        while True:
            try:
                yield await self.iterate()
            except StopAsyncIteration:
                break

    async def __anext__(self):
        if self._idx < self._ct:
            inst = self._result_cache[self._idx]
            self._idx += 1
            return inst
        elif self._populated:
            raise StopAsyncIteration

        obj = await self.iterate()
        self._result_cache.append(obj)
        self._ct += 1
        self._idx += 1
        return obj

    async def fill_cache(self, n=None):
        # TODO: cursor.fetchall
        # if n is None cursor.fetchall else cursor.fetchmany(n)
        n = n or float('Inf')
        if n < 0:
            raise ValueError('Negative values are not supported.')
        self._idx = self._ct
        while not self._populated and (n > self._ct):
            try:
                await self.__anext__()
            except StopAsyncIteration:
                break


class AioExtQueryResultWrapper(AioQueryResultWrapper,
                               ExtQueryResultWrapper):
    pass


class AioTuplesQueryResultWrapper(AioExtQueryResultWrapper,
                                  TuplesQueryResultWrapper):
    pass


class AioNaiveQueryResultWrapper(AioExtQueryResultWrapper,
                                 NaiveQueryResultWrapper):
    pass


class AioDictQueryResultWrapper(AioExtQueryResultWrapper,
                                DictQueryResultWrapper):
    pass


class AioModelQueryResultWrapper(AioQueryResultWrapper,
                                 ModelQueryResultWrapper):
    pass


class AioAggregateQueryResultWrapper(AioModelQueryResultWrapper,
                                     AggregateQueryResultWrapper):

    async def iterate(self):
        if self._row:
            row = self._row.pop()
        else:
            row = await self.cursor.fetchone()

        if not row:
            self._populated = True
            if not getattr(self.cursor, 'name', None):
                await self.cursor.close()
            raise StopAsyncIteration
        elif not self._initialized:
            self.initialize(self.cursor.description)
            self._initialized = True

        def _get_pk(instance):
            if instance._meta.composite_key:
                return tuple([
                    instance._data[field_name]
                    for field_name in instance._meta.primary_key.field_names])
            return instance._get_pk_value()

        identity_map = {}
        _constructed = self.construct_instances(row)
        primary_instance = _constructed[self.model]
        for model_or_alias, instance in _constructed.items():
            identity_map[model_or_alias] = OrderedDict()
            identity_map[model_or_alias][_get_pk(instance)] = instance

        model_data = self.read_model_data(row)
        while True:
            cur_row = await self.cursor.fetchone()
            if cur_row is None:
                break

            duplicate_models = set()
            cur_row_data = self.read_model_data(cur_row)
            for model_class, data in cur_row_data.items():
                if model_data[model_class] == data:
                    duplicate_models.add(model_class)

            if not duplicate_models:
                self._row.append(cur_row)
                break

            different_models = self.all_models - duplicate_models

            new_instances = self.construct_instances(cur_row, different_models)
            for model_or_alias, instance in new_instances.items():
                # Do not include any instances which are comprised solely of
                # NULL values.
                all_none = True
                for value in instance._data.values():
                    if value is not None:
                        all_none = False
                if not all_none:
                    identity_map[model_or_alias][_get_pk(instance)] = instance

        stack = [self.model]
        instances = [primary_instance]
        while stack:
            current = stack.pop()
            if current not in self.join_meta:
                continue

            for join in self.join_meta[current]:
                try:
                    metadata, attr = self.source_to_dest[current][join.dest]
                except KeyError:
                    continue

                if metadata.is_backref or metadata.is_self_join:
                    for instance in identity_map[current].values():
                        setattr(instance, attr, [])

                    if join.dest not in identity_map:
                        continue

                    for pk, inst in identity_map[join.dest].items():
                        if pk is None:
                            continue
                        try:
                            # XXX: if no FK exists, unable to join.
                            joined_inst = identity_map[current][
                                inst._data[metadata.foreign_key.name]]
                        except KeyError:
                            continue

                        getattr(joined_inst, attr).append(inst)
                        instances.append(inst)
                elif attr:
                    if join.dest not in identity_map:
                        continue

                    for pk, instance in identity_map[current].items():
                        # XXX: if no FK exists, unable to join.
                        joined_inst = identity_map[join.dest][
                            instance._data[metadata.foreign_key.name]]
                        setattr(
                            instance,
                            metadata.foreign_key.name,
                            joined_inst)
                        instances.append(joined_inst)

                stack.append(join.dest)

        for instance in instances:
            instance._prepare_instance()

        return primary_instance
