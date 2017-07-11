import re
from playhouse.fields import (ManyToManyField, ManyToManyQuery,
                              ManyToManyFieldDescriptor, DeferredThroughModel)
from peewee import Proxy, ForeignKeyField, SelectQuery, Model, SQL

from .model import AioModel
from .query import AioSelectQuery


class AioManyToManyField(ManyToManyField):

    def _get_descriptor(self):
        return AioManyToManyFieldDescriptor(self)

    def add_to_class(self, model_class, name):
        if isinstance(self._through_model, Proxy):
            def callback(through_model):
                self._through_model = through_model
                self.add_to_class(model_class, name)
            self._through_model.attach_callback(callback)
            return
        elif isinstance(self._through_model, DeferredThroughModel):
            self._through_model.set_field(model_class, self, name)
            return

        self.name = name
        self.model_class = model_class
        if not self.verbose_name:
            self.verbose_name = re.sub('_+', ' ', name).title()
        setattr(model_class, name, self._get_descriptor())

        if not self._is_backref:
            backref = AioManyToManyField(
                self.model_class,
                through_model=self._through_model,
                _is_backref=True)
            related_name = self._related_name or model_class._meta.name + 's'
            backref.add_to_class(self.rel_model, related_name)

    def get_through_model(self):
        if not self._through_model:
            lhs, rhs = self.get_models()
            tables = [model._meta.db_table for model in (lhs, rhs)]

            class Meta:
                database = self.model_class._meta.database
                db_table = '%s_%s_through' % tuple(tables)
                indexes = (
                    ((lhs._meta.name, rhs._meta.name),
                     True),)
                validate_backrefs = False

            attrs = {
                lhs._meta.name: ForeignKeyField(rel_model=lhs),
                rhs._meta.name: ForeignKeyField(rel_model=rhs)}
            attrs['Meta'] = Meta

            self._through_model = type(
                '%s%sThrough' % (lhs.__name__, rhs.__name__),
                (AioModel,),
                attrs)

        return self._through_model


class AioManyToManyFieldDescriptor(ManyToManyFieldDescriptor):

    def __get__(self, instance, instance_type=None):
        if instance is not None:
            return (AioManyToManyQuery(instance, self, self.rel_model)
                    .select()
                    .join(self.through_model)
                    .join(self.model_class)
                    .where(self.src_fk == instance))
        return self.field

    def __set__(self, instance, value):
        raise NotImplementedError('Use `set()` coroutine instead!')
        # query = self.__get__(instance)
        # query.add(value, clear_existing=True)


class AioManyToManyQuery(AioSelectQuery, ManyToManyQuery):

    # TODO
    def _id_list(self, model_or_id_list):
        if isinstance(model_or_id_list[0], Model):
            return [obj.get_id() for obj in model_or_id_list]
        return model_or_id_list

    async def set(self, value):
        await self.add(value, clear_existing=True)

    async def add(self, value, clear_existing=False):
        if clear_existing:
            await self.clear()

        fd = self._field_descriptor
        if isinstance(value, SelectQuery):
            query = value.select(
                SQL(str(self._instance.get_id())),
                fd.rel_model._meta.primary_key)
            await fd.through_model.insert_from(
                fields=[fd.src_fk, fd.dest_fk],
                query=query).execute()
        else:
            if not isinstance(value, (list, tuple)):
                value = [value]
            if not value:
                return
            inserts = [{
                fd.src_fk.name: self._instance.get_id(),
                fd.dest_fk.name: rel_id}
                for rel_id in self._id_list(value)]
            await fd.through_model.insert_many(inserts).execute()

    async def remove(self, value):
        fd = self._field_descriptor
        if isinstance(value, SelectQuery):
            subquery = value.select(value.model_class._meta.primary_key)
            return await (fd.through_model
                            .delete()
                            .where(
                                (fd.dest_fk << subquery) &
                                (fd.src_fk == self._instance.get_id()))
                            .execute())
        else:
            if not isinstance(value, (list, tuple)):
                value = [value]
            if not value:
                return
            return await (fd.through_model
                            .delete()
                            .where(
                                (fd.dest_fk << self._id_list(value)) &
                                (fd.src_fk == self._instance.get_id()))
                            .execute())

    async def clear(self):
        return await (self._field_descriptor.through_model
                      .delete()
                      .where(self._field_descriptor.src_fk == self._instance)
                      .execute())
