from aiopeewee import AioModel, AioMySQLDatabase
from peewee import (ForeignKeyField, IntegerField, CharField,
                    DateTimeField, TextField, PrimaryKeyField)


db = AioMySQLDatabase('test', host='127.0.0.1', port=3306,
                      user='root', password='')


class Count(AioModel):
    id = IntegerField(primary_key=True)
    data = CharField(max_length=64, null=False)
    count = IntegerField(default=0)
    created_at = DateTimeField()
    updated_at = DateTimeField()

    class Meta:
        database = db


class User(AioModel):
    username = CharField()

    class Meta:
        database = db

    def prepared(self):
        self.foo = self.username

    @classmethod
    async def create_users(cls, n):
        for i in range(n):
            await cls.create(username='u%d' % (i + 1))


class Blog(AioModel):
    user = ForeignKeyField(User)
    title = CharField(max_length=25)
    content = TextField(default='')
    pub_date = DateTimeField(null=True)
    pk = PrimaryKeyField()

    class Meta:
        database = db

    def prepared(self):
        self.foo = self.title
