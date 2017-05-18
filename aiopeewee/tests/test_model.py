import pytest

from aitertools import alist
from datetime import datetime
from aiopeewee import AioModel, AioMySQLDatabase
from peewee import (ForeignKeyField, IntegerField, CharField,
                    DateTimeField, TextField, PrimaryKeyField,
                    SQL)


db = AioMySQLDatabase('test', host='database', port=3306,
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


async def create_users_blogs(n=10, nb=5):
    for i in range(n):
        u = await User.create(username='u%d' % i)
        for j in range(nb):
            b = await Blog.create(title='b-%d-%d' % (i, j),
                                  content=str(j), user=u)


async def test_table_creation(loop):
    await db.connect(loop)

    assert await Count.table_exists() is False

    await Count.create_table()
    assert await Count.table_exists() is True

    await Count.drop_table()
    assert await Count.table_exists() is False

    await db.close()


async def test_crud(loop):
    await db.connect(loop)
    await Count.create_table(fail_silently=True)

    now = datetime.now()
    await Count.create(id=1, data='test', created_at=now, updated_at=now)
    await Count.create(id=2, data='test', created_at=now, updated_at=now)

    # TODO
    await Count.drop_table()
    await db.close()


async def test_query(loop):
    await db.connect(loop)
    await Count.create_table(fail_silently=True)

    now = datetime.now()
    await Count.create(id=1, data='test', created_at=now, updated_at=now)
    await Count.create(id=2, data='test', created_at=now, updated_at=now)

    c = await Count.select().count()
    assert c == 2, ''

    data = [i async for i in Count.select()]
    assert len(data) == 2, ''

    data = [i async for i in Count.select().where(Count.id > 0)]
    assert len(data) == 2, ''

    data = [i async for i in Count.select().limit(1)]
    assert len(data) == 1, ''

    data = [i async for i in Count.select().order_by(Count.id.desc())]
    assert data[0].id == 2

    record = await Count.select().order_by(Count.id.desc()).first()
    assert record.id == 2

    # TODO
    # t = await Count.select().order_by(Count.id.desc()).first()
    # await t.delete_instance()

    # await Count.update(data='12345')
    # t = await Count.select().order_by(Count.id.desc()).first()
    # assert t.data == '12345', ''

    await Count.drop_table()
    await db.close()


async def test_select(loop):
    await db.connect(loop)
    await db.create_tables([User, Blog], safe=True)
    await create_users_blogs()

    users = (User.select()
                 .where(User.username << ['u0', 'u5'])
                 .order_by(User.username))
    assert [u.username async for u in users] == ['u0', 'u5']

    blogs = Blog.select().join(User).where(
        (User.username << ['u0', 'u3']) &
        (Blog.content == '4')
    ).order_by(Blog.title)
    assert [b.title async for b in blogs] == ['b-0-4', 'b-3-4']

    users = User.select().paginate(2, 3)
    assert [u.username async for u in users] == ['u3', 'u4', 'u5']

    await db.drop_tables([User, Blog], safe=True)
    await db.close()


async def test_select_all(loop):
    await db.connect(loop)
    await db.create_tables([User, Blog], safe=True)
    await create_users_blogs(2, 2)

    all_cols = SQL('*')
    query = Blog.select(all_cols)
    blogs = [blog async for blog in query.order_by(Blog.pk)]
    assert [b.title for b in blogs] == ['b-0-0', 'b-0-1', 'b-1-0', 'b-1-1']

    assert [(await b.user).username for b in blogs] == ['u0', 'u0', 'u1', 'u1']

    # TODO: await blogs[0] fails with cannot await again
    # must be a caching issue

    await db.drop_tables([User, Blog], safe=True)
    await db.close()


async def test_select_all_fetchall(loop):
    from aitertools import aiter
    await db.connect(loop)
    await db.create_tables([User, Blog], safe=True)
    await create_users_blogs(2, 2)

    all_cols = SQL('*')

    query0 = Blog.select(all_cols).order_by(Blog.pk)
    blogs0 = await alist(query0)

    query1 = Blog.select(all_cols).order_by(Blog.pk)
    blogs1 = await query1

    assert isinstance(blogs0, list)
    assert isinstance(blogs1, list)
    assert [b.title for b in blogs0] == ['b-0-0', 'b-0-1', 'b-1-0', 'b-1-1']
    assert [b.title for b in blogs1] == ['b-0-0', 'b-0-1', 'b-1-0', 'b-1-1']
    assert blogs0 == blogs1

    await db.drop_tables([User, Blog], safe=True)
    await db.close()


async def test_insert(loop):
    await db.connect(loop)
    await db.create_tables([User], safe=True)

    iq = User.insert(username='u1')
    assert await User.select().count() == 0

    uid = await iq.execute()
    assert uid > 0

    assert await User.select().count() == 1
    u = await User.get(User.id==uid)
    u.username == 'u1'

    with pytest.raises(KeyError):
        await User.insert(doesnotexist='invalid')

    await db.drop_tables([User], safe=True)
    await db.close()


async def test_insert_many(loop):
    await db.connect(loop)
    await db.create_tables([User], safe=True)

    iq = User.insert_many([
        {'username': 'u1'},
        {'username': 'u2'},
        {'username': 'u3'},
        {'username': 'u4'}])
    assert await iq.execute() is True

    await User.select().count() == 4

    sq = User.select(User.username).order_by(User.username)
    assert [u.username async for u in sq] == ['u1', 'u2', 'u3', 'u4']

    iq = User.insert_many([{'username': 'u5'}])
    await iq.execute() is True
    await User.select().count() == 5

    iq = await User.insert_many([
        {User.username: 'u6'},
        {User.username: 'u7'},
        {'username': 'u8'}])

    sq = User.select(User.username).order_by(User.username)
    assert [u.username async for u in sq] == ['u1', 'u2', 'u3', 'u4',
                                              'u5', 'u6', 'u7', 'u8']

    await db.drop_tables([User], safe=True)
    await db.close()


async def test_delete(loop):
    await db.connect(loop)
    await db.create_tables([User], safe=True)

    await User.create_users(5)
    dq = User.delete().where(User.username << ['u1', 'u2', 'u3'])
    assert await User.select().count() == 5
    nr = await dq
    assert nr == 3
    assert [u.username async for u in User.select()] == ['u4', 'u5']

    await db.drop_tables([User], safe=True)
    await db.close()


async def test_related_name(loop):
    await db.connect(loop)
    await db.create_tables([User, Blog], safe=True)

    u1 = await User.create(username='u1')
    u2 = await User.create(username='u2')
    b11 = await Blog.create(user=u1, title='b11')
    b12 = await Blog.create(user=u1, title='b12')
    b2 = await Blog.create(user=u2, title='b2')

    assert [b.title async for b in u1.blog_set.order_by(Blog.title)] == ['b11', 'b12']
    assert [b.title async for b in u2.blog_set.order_by(Blog.title)] == ['b2']

    await db.drop_tables([User, Blog], safe=True)
    await db.close()


async def test_saving(loop):
    await db.connect(loop)
    await db.create_tables([User], safe=True)

    assert await User.select().count() == 0

    u = User(username='u1')
    assert await u.save() == 1
    u.username = 'u2'
    assert await u.save() == 1

    assert await User.select().count() == 1

    assert await u.delete_instance() == 1
    assert await u.save() == 0

    await db.drop_tables([User], safe=True)
    await db.close()
