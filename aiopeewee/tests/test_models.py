import sys
import pytest

from utils import assert_query_count, assert_queries_equal
from functools import partial

from models import *
from peewee import CharField, IntegerField, ForeignKeyField
from peewee import SQL, fn, R, QueryCompiler
from peewee import ModelOptions

from aiopeewee import AioModel as Model
from aiopeewee import AioMySQLDatabase
from aiopeewee.utils import alist


# in_memory_db = database_initializer.get_in_memory_database()
# supports_tuples = sqlite3.sqlite_version_info >= (3, 15, 0)


pytestmark = pytest.mark.asyncio


async def create_users_blogs(n=10, nb=5):
    for i in range(n):
        u = await User.create(username=f'u{i}')
        for j in range(nb):
            b = await Blog.create(title=f'b-{i}-{j}', content=str(j), user=u)


async def test_select(flushdb):
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


async def test_select_all(flushdb):
    await create_users_blogs(2, 2)
    all_cols = SQL('*')
    query = Blog.select(all_cols)
    blogs = [blog async for blog in query.order_by(Blog.pk)]
    assert [b.title for b in blogs] == ['b-0-0', 'b-0-1', 'b-1-0', 'b-1-1']
    assert [(await b.user).username for b in blogs] == ['u0', 'u0', 'u1', 'u1']


async def test_select_subquery(flushdb):
    # 10 users, 5 blogs each
    await create_users_blogs(5, 3)

    # delete user 2's 2nd blog
    await Blog.delete().where(Blog.title == 'b-2-2')

    subquery = (Blog.select(fn.Count(Blog.pk))
                    .where(Blog.user == User.id)
                    .group_by(Blog.user))
    users = User.select(User, subquery.alias('ct')).order_by(R('ct'), User.id)

    expected = [('u2', 2),
                ('u0', 3),
                ('u1', 3),
                ('u3', 3),
                ('u4', 3)]
    assert [(x.username, x.ct) async for x in users] == expected


async def test_select_with_bind_to(flushdb):
    await create_users_blogs(1, 1)

    blog = await Blog.select(
        Blog,
        User,
        (User.username == 'u0').alias('is_u0').bind_to(User),
        (User.username == 'u1').alias('is_u1').bind_to(User)
    ).join(User).get()

    assert blog.user.is_u0 == 1
    assert blog.user.is_u1 == 0


async def test_scalar(flushdb):
    await User.create_users(5)

    users = User.select(fn.Count(User.id)).scalar()
    assert await users == 5

    users = User.select(fn.Count(User.id)).where(User.username << ['u1', 'u2'])
    assert await users.scalar() == 2
    assert await users.scalar(True) == (2,)

    users = User.select(fn.Count(User.id)).where(User.username == 'not-here')
    assert await users.scalar() == 0
    assert await users.scalar(True) == (0,)

    users = User.select(fn.Count(User.id), fn.Count(User.username))
    assert await users.scalar() == 5
    assert await users.scalar(True) == (5, 5)

    await User.create(username='u1')
    await User.create(username='u2')
    await User.create(username='u3')
    await User.create(username='u99')
    users = User.select(fn.Count(fn.Distinct(User.username))).scalar()
    assert await users == 6


async def test_update(flushdb):
    await User.create_users(5)
    uq = (User.update(username='u-edited')
              .where(User.username << ['u1', 'u2', 'u3']))

    sq = User.select().order_by(User.id)
    assert [u.username async for u in sq] == ['u1', 'u2', 'u3', 'u4', 'u5']

    await uq.execute()
    sq = User.select().order_by(User.id)
    assert [u.username async for u in sq] == ['u-edited', 'u-edited',
                                              'u-edited', 'u4', 'u5']

    with pytest.raises(KeyError):
        await User.update(doesnotexist='invalid')


async def test_update_subquery(flushdb):
    await User.create_users(3)
    u1, u2, u3 = [user async for user in User.select().order_by(User.id)]
    for i in range(4):
        await Blog.create(title=f'b{i}', user=u1)
    for i in range(2):
        await Blog.create(title=f'b{i}', user=u3)

    subquery = Blog.select(fn.COUNT(Blog.pk)).where(Blog.user == User.id)
    query = User.update(username=subquery)
    normal_compiler = QueryCompiler('"', '?', {}, {})
    sql, params = normal_compiler.generate_update(query)
    assert sql == ('UPDATE "users" SET "username" = ('
                   'SELECT COUNT("t2"."pk") FROM "blog" AS t2 '
                   'WHERE ("t2"."user_id" = "users"."id"))')
    assert await query == 3

    usernames = [u.username async for u in User.select().order_by(User.id)]
    assert usernames == ['4', '0', '2']


async def test_insert(flushdb):
    iq = User.insert(username='u1')
    assert await User.select().count() == 0
    uid = await iq.execute()
    assert uid > 0
    assert await User.select().count() == 1
    u = await User.get(User.id==uid)
    assert u.username == 'u1'

    with pytest.raises(KeyError):
        await User.insert(doesnotexist='invalid')


async def test_insert_from(flushdb):
    u0, u1, u2 = [await User.create(username=f'U{i}') for i in range(3)]

    subquery = (User
                .select(fn.LOWER(User.username))
                .where(User.username << ['U0', 'U2']))
    iq = User.insert_from([User.username], subquery)
    normal_compiler = QueryCompiler('"', '?', {}, {})
    sql, params = normal_compiler.generate_insert(iq)
    assert sql == ('INSERT INTO "users" ("username") '
                   'SELECT LOWER("t2"."username") FROM "users" AS t2 '
                   'WHERE ("t2"."username" IN (?, ?))')
    assert params == ['U0', 'U2']

    await iq.execute()
    usernames = sorted([u.username async for u in User.select()])
    assert usernames == ['U0', 'U1', 'U2', 'u0', 'u2']


async def test_insert_many_validates_fields_by_default():
    assert User.insert_many([])._validate_fields is True


async def test_insert_many_without_field_validation():
    iq = User.insert_many([], validate_fields=False)
    assert iq._validate_fields is False


async def test_delete(flushdb):
    await User.create_users(5)
    dq = User.delete().where(User.username << ['u1', 'u2', 'u3'])
    assert await User.select().count() == 5
    nr = await dq.execute()
    assert nr == 3
    assert [u.username async for u in User.select()] == ['u4', 'u5']


async def test_limits_offsets(flushdb):
    for i in range(10):
        await User.create(username=f'u{i}')
    sq = User.select().order_by(User.id)

    offset_no_lim = sq.offset(3)
    expected = [f'u{i}' for i in range(3, 10)]
    assert [u.username async for u in offset_no_lim] == expected

    offset_with_lim = sq.offset(5).limit(3)
    expected = [f'u{i}' for i in range(5, 8)]
    assert [u.username async for u in offset_with_lim] == expected


async def test_raw_fn(flushdb):
    await create_users_blogs(3, 2)  # 3 users, 2 blogs each.
    query = User.raw('select count(1) as ct from blog group by user_id')
    results = [x.ct async for x in query]
    assert results == [2, 2, 2]


async def test_insert_many(flushdb):
    if db.insert_many:
        with assert_query_count(1):
            iq = User.insert_many([
                {'username': 'u1'},
                {'username': 'u2'},
                {'username': 'u3'},
                {'username': 'u4'}])
            assert await iq.execute()
    else:
        with assert_query_count(4):
            iq = User.insert_many([
                {'username': 'u1'},
                {'username': 'u2'},
                {'username': 'u3'},
                {'username': 'u4'}])
            assert await iq.execute()

    assert await User.select().count() == 4

    sq = User.select(User.username).order_by(User.username)
    assert [u.username async for u in sq] == ['u1', 'u2', 'u3', 'u4']

    iq = User.insert_many([{'username': 'u5'}])
    assert await iq.execute()
    assert await User.select().count() == 5

    iq = await User.insert_many([
        {User.username: 'u6'},
        {User.username: 'u7'},
        {'username': 'u8'}]).execute()

    sq = User.select(User.username).order_by(User.username)
    exp = ['u1', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8']
    assert [u.username async for u in sq] == exp


async def test_noop_query(flushdb):
    query = User.noop()
    with assert_query_count(1) as qc:
        result = [row async for row in query]

    assert result == []


async def test_insert_many_fallback(flushdb):
    # Simulate database not supporting multiple insert (older versions of
    # sqlite).
    db.insert_many = False
    with assert_query_count(4):
        iq = User.insert_many([
            {'username': 'u1'},
            {'username': 'u2'},
            {'username': 'u3'},
            {'username': 'u4'}])
        assert await iq

    assert await User.select().count() == 4


async def test_raw(flushdb):
    await User.create_users(3)
    interpolation = db.interpolation

    with assert_query_count(1):
        query = 'select * from users where username IN (%s, %s)' % (
            interpolation, interpolation)
        rq = User.raw(query, 'u1', 'u3')

        assert [u.username async for u in rq] == ['u1', 'u3']

        # iterate again
        assert [u.username async for u in rq] == ['u1', 'u3']

    query = ('select id, username, %s as secret '
             'from users where username = %s')
    rq = User.raw(query % (interpolation, interpolation),
                  'sh', 'u2')
    assert [u.secret async for u in rq] == ['sh']
    assert [u.username async for u in rq] == ['u2']

    rq = User.raw('select count(id) from users')
    assert await rq.scalar() == 3

    rq = User.raw('select username from users').tuples()
    assert [r async for r in rq] == [('u1',), ('u2',), ('u3',)]


# async def test_insert_empty(flushdb):
#     query = EmptyModel.insert()

#     # TODO
#     # sql, params = db.compiler().generate_insert(query)
#     # if isinstance(db, AioMySQLDatabase):
#     #     assert sql == ('INSERT INTO "emptymodel" ("emptymodel"."id") '
#     #                    'VALUES (DEFAULT)')
#     # else:
#     #     assert sql == 'INSERT INTO "emptymodel" DEFAULT VALUES'
#     # assert params == []

#     # Verify the query works.
#     pk = await query.execute()
#     em = await EmptyModel.get(EmptyModel.id == pk)

#     # Verify we can also use `create()`.
#     em2 = await EmptyModel.create()
#     assert await EmptyModel.select().count() == 2


async def test_no_pk(flushdb):
    obj = await NoPKModel.create(data='1')
    assert await NoPKModel.select(fn.COUNT('1')).scalar() == 1

    res = await (NoPKModel.update(data='1-e')
                          .where(NoPKModel.data == '1'))
    assert res == 1
    assert await NoPKModel.select(fn.COUNT('1')).scalar() == 1

    await NoPKModel(data='2').save()
    await NoPKModel(data='3').save()

    result = [obj.data async for obj in
              NoPKModel.select().order_by(NoPKModel.data)]
    assert result == ['1-e', '2', '3']


async def test_related_name(flushdb):
    u1 = await User.create(username='u1')
    u2 = await User.create(username='u2')
    b11 = await Blog.create(user=u1, title='b11')
    b12 = await Blog.create(user=u1, title='b12')
    b2 = await Blog.create(user=u2, title='b2')

    sq = u1.blog_set.order_by(Blog.title)
    assert [b.title async for b in sq] == ['b11', 'b12']

    sq = u2.blog_set.order_by(Blog.title)
    assert [b.title async for b in sq] == ['b2']


async def test_related_name_collision(flushdb):
    class Foo(TestModel):
        f1 = CharField()

    with pytest.raises(AttributeError):
        class FooRel(TestModel):
            foo = ForeignKeyField(Foo, related_name='f1')


async def test_callable_related_name():
    class Foo(TestModel):
        pass

    def rel_name(field):
        return '{}_{}_ref'.format(field.model_class._meta.name, field.name)

    class Bar(TestModel):
        fk1 = ForeignKeyField(Foo, related_name=rel_name)
        fk2 = ForeignKeyField(Foo, related_name=rel_name)

    class Baz(Bar):
        pass

    assert Foo.bar_fk1_ref.rel_model is Bar
    assert Foo.bar_fk2_ref.rel_model is Bar
    assert Foo.baz_fk1_ref.rel_model is Baz
    assert Foo.baz_fk2_ref.rel_model is Baz
    assert not hasattr(Foo, 'bar_set')
    assert not hasattr(Foo, 'baz_set')


async def test_fk_exceptions(flushdb):
    c1 = await Category.create(name='c1')
    c2 = await Category.create(parent=c1, name='c2')
    assert c1.parent is None
    assert c2.parent is c1

    c2_db = await Category.get(Category.id == c2.id)
    assert await c2_db.parent == c1

    u = await User.create(username='u1')
    b = await Blog.create(user=u, title='b')
    b2 = Blog(title='b2')

    assert b.user is u
    with pytest.raises(User.DoesNotExist):
        await b2.user


# async def test_fk_cache_invalidated(flushdb):
#     u1 = await User.create(username='u1')
#     u2 = await User.create(username='u2')
#     b = await Blog.create(user=u1, title='b')

#     blog = await Blog.get(Blog.pk == b)
#     with assert_query_count(1):
#         assert (await blog.user).id == u1.id

#     blog.user = u2.id
#     with assert_query_count(1):
#         assert (await blog.user).id == u2.id

#     # No additional query.
#     blog.user = u2.id
#     with assert_query_count(0):
#         assert (await blog.user).id == u2.id


async def test_fk_ints(flushdb):
    c1 = await Category.create(name='c1')
    c2 = await Category.create(name='c2', parent=c1.id)
    c2_db = await Category.get(Category.id == c2.id)
    assert await c2_db.parent == c1


async def test_fk_object_id(flushdb):
    c1 = await Category.create(name='c1')
    c2 = await Category.create(name='c2')
    c2.parent_id = c1.id
    await c2.save()
    assert c2.parent == c1
    c2_db = await Category.get(Category.name == 'c2')
    assert await c2_db.parent == c1


async def test_fk_caching(flushdb):
    c1 = await Category.create(name='c1')
    c2 = await Category.create(name='c2', parent=c1)
    c2_db = await Category.get(Category.id == c2.id)

    with assert_query_count(1):
        parent = await c2_db.parent
        assert parent == c1

        # TODO it should be awaitable again
        parent = c2_db.parent


async def test_related_id(flushdb):
    u1 = await User.create(username='u1')
    u2 = await User.create(username='u2')
    for u in [u1, u2]:
        for j in range(2):
            await Blog.create(user=u, title='%s-%s' % (u.username, j))

    with assert_query_count(1):
        query = Blog.select().order_by(Blog.pk)
        user_ids = [blog.user_id async for blog in query]

    assert user_ids == [u1.id, u1.id, u2.id, u2.id]

    p1 = await Category.create(name='p1')
    p2 = await Category.create(name='p2')
    c1 = await Category.create(name='c1', parent=p1)
    c2 = await Category.create(name='c2', parent=p2)

    with assert_query_count(1):
        query = Category.select().order_by(Category.id)
        expected = [None, None, p1.id, p2.id]
        assert [cat.parent_id async for cat in query] == expected


async def test_fk_object_id(flushdb):
    u = await User.create(username='u')
    b = await Blog.create(user_id=u.id, title='b1')
    assert b._data['user'] == u.id
    assert 'user' not in b._obj_cache

    with assert_query_count(1):
        u_db = await b.user
        assert u_db.id == u.id

    b_db = await Blog.get(Blog.pk == b.pk)
    with assert_query_count(0):
        assert b_db.user_id == u.id

    u2 = await User.create(username='u2')
    await Blog.create(user=u, title='b1x')
    await Blog.create(user=u2, title='b2')

    q = Blog.select().where(Blog.user_id == u2.id)
    assert await q.count(), 1
    assert (await q.get()).title == 'b2'

    q = Blog.select(Blog.pk, Blog.user_id).where(Blog.user_id == u.id)
    assert await q.count() == 2
    result = await q.order_by(Blog.pk).first()
    assert result.user_id == u.id
    with assert_query_count(1):
        assert (await result.user).id == u.id


async def test_object_id_descriptor_naming():
    class Person(Model):
        pass

    class Foo(Model):
        me = ForeignKeyField(Person, db_column='me', related_name='foo1')
        another = ForeignKeyField(Person, db_column='_whatever_',
                                  related_name='foo2')
        another2 = ForeignKeyField(Person, db_column='person_id',
                                   related_name='foo3')
        plain = ForeignKeyField(Person, related_name='foo4')

    assert Foo.me is Foo.me_id
    assert Foo.another is Foo._whatever_
    assert Foo.another2 is Foo.person_id
    assert Foo.plain is Foo.plain_id

    with pytest.raises(AttributeError):
        Foo.another_id

    with pytest.raises(AttributeError):
        Foo.another2_id


async def test_category_select_related_alias(flushdb):
    g1 = await Category.create(name='g1')
    g2 = await Category.create(name='g2')

    p1 = await Category.create(name='p1', parent=g1)
    p2 = await Category.create(name='p2', parent=g2)

    c1 = await Category.create(name='c1', parent=p1)
    c11 = await Category.create(name='c11', parent=p1)
    c2 = await Category.create(name='c2', parent=p2)

    with assert_query_count(1):
        Grandparent = Category.alias()
        Parent = Category.alias()
        sq = (Category
              .select(Category, Parent, Grandparent)
              .join(Parent, on=(Category.parent == Parent.id))
              .join(Grandparent, on=(Parent.parent == Grandparent.id))
              .where(Grandparent.name == 'g1')
              .order_by(Category.name))

        result = [(c.name, c.parent.name, c.parent.parent.name)
                  async for c in sq]
        assert result == [('c1', 'p1', 'g1'), ('c11', 'p1', 'g1')]


async def test_save_fk(flushdb):
    blog = Blog(title='b1', content='')
    blog.user = User(username='u1')
    await blog.user.save()
    with assert_query_count(1):
        await blog.save()

    with assert_query_count(1):
        blog_db = await (Blog
                   .select(Blog, User)
                   .join(User)
                   .where(Blog.pk == blog.pk)
                   .get())
        assert blog_db.user.username == 'u1'


async def test_creation(flushdb):
    await User.create_users(10)
    assert await User.select().count() == 10


async def test_saving(flushdb):
    assert await User.select().count() == 0

    u = User(username='u1')
    assert await u.save() == 1
    u.username = 'u2'
    assert await u.save() == 1

    assert await User.select().count() == 1

    assert await u.delete_instance() == 1
    assert await u.save() == 0


async def test_modify_model_cause_it_dirty(flushdb):
    u = User(username='u1')
    await u.save()
    assert u.is_dirty() is False

    u.username = 'u2'
    assert u.is_dirty() is True
    assert u.dirty_fields == [User.username]

    await u.save()
    assert u.is_dirty() is False

    b = await Blog.create(user=u, title='b1')
    assert b.is_dirty() is False

    b.user = u
    assert b.is_dirty() is True
    assert b.dirty_fields == [Blog.user]


async def test_dirty_from_query(flushdb):
    u1 = await User.create(username='u1')
    b1 = await Blog.create(title='b1', user=u1)
    b2 = await Blog.create(title='b2', user=u1)

    u_db = await User.get()
    assert u_db.is_dirty() is False

    b_with_u = await (Blog.select(Blog, User)
                          .join(User)
                          .where(Blog.title == 'b2')
                          .get())
    assert b_with_u.is_dirty() is False
    assert b_with_u.user.is_dirty() is False

    u_with_blogs_q = (User.select(User, Blog)
                          .join(Blog)
                          .order_by(Blog.title)
                          .aggregate_rows())
    u_with_blogs = (await alist(u_with_blogs_q))[0]
    assert u_with_blogs.is_dirty() is False

    for blog in u_with_blogs.blog_set:
        assert blog.is_dirty() is False

    b_with_users = await alist(Blog
                    .select(Blog, User)
                    .join(User)
                    .order_by(Blog.title)
                    .aggregate_rows())
    b1, b2 = b_with_users
    assert b1.is_dirty() is False
    assert b1.user.is_dirty() is False
    assert b2.is_dirty() is False
    assert b2.user.is_dirty() is False


async def test_save_only(flushdb):
    u = await User.create(username='u')
    b = await Blog.create(user=u, title='b1', content='ct')
    b.title = 'b1-edit'
    b.content = 'ct-edit'

    await b.save(only=[Blog.title])

    b_db = await Blog.get(Blog.pk == b.pk)
    assert b_db.title == 'b1-edit'
    assert b_db.content == 'ct'

    b = Blog(user=u, title='b2', content='foo')
    await b.save(only=[Blog.user, Blog.title])

    b_db = await Blog.get(Blog.pk == b.pk)

    assert b_db.title =='b2'
    assert b_db.content == ''


async def test_save_only_dirty_fields(flushdb):
    u = await User.create(username='u1')
    b = await Blog.create(title='b1', user=u, content='huey')
    b_db = await Blog.get(Blog.pk == b.pk)
    b.title = 'baby huey'
    await b.save(only=b.dirty_fields)
    b_db.content = 'mickey-nugget'
    await b_db.save(only=b_db.dirty_fields)
    saved = await Blog.get(Blog.pk == b.pk)
    assert saved.title == 'baby huey'
    assert saved.content == 'mickey-nugget'


async def test_save_dirty_auto(flushdb):
    User._meta.only_save_dirty = True
    Blog._meta.only_save_dirty = True
    try:
        with assert_query_count(2) as query_logger:
            u = await User.create(username='u1')
            b = await Blog.create(title='b1', user=u)

        # The default value for the blog content will be saved as well.
        params = [params for _, params in query_logger.queries()]
        assert params == [['u1'], [u.id, 'b1', '']]

        with assert_query_count(0):
            assert await u.save() is False
            assert await b.save() is False

        u.username = 'u1-edited'
        b.title = 'b1-edited'
        with assert_query_count(1) as query_logger:
            assert await u.save() == 1

        sql, params = query_logger.queries()[0]
        assert sql.startswith('UPDATE')
        assert params == ['u1-edited', u.id]

        with assert_query_count(1) as query_logger:
            assert await b.save() == 1

        sql, params = query_logger.queries()[0]
        assert sql.startswith('UPDATE')
        assert params == ['b1-edited', b.pk]
    finally:
        User._meta.only_save_dirty = False
        Blog._meta.only_save_dirty = False


async def test_zero_id(flushdb):
    if isinstance(db, MySQLDatabase):
        # Need to explicitly tell MySQL it's OK to use zero.
        await db.execute_sql("SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO'")
    query = 'insert into users (id, username) values ({}, {})'.format(
        db.interpolation, db.interpolation)

    await db.execute_sql(query, (0, 'foo'))
    await Blog.insert(title='foo2', user=0)

    u = await User.get(User.id == 0)
    b = await Blog.get(Blog.user == u)

    assert u == u
    assert u == await b.user


async def test_saving_via_create_gh111(flushdb):
    with assert_query_count(2) as qlh:
        u = await User.create(username='u')
        b = await Blog.create(title='foo', user=u)
        last_sql = qlh.queries()[-1]
        assert 'pub_date' not in last_sql
        assert b.pub_date is None

    with assert_query_count(1) as qlh:
        b2 = Blog(title='foo2', user=u)
        await b2.save()

        last_sql = qlh.queries()[-1]
        assert 'pub_date' not in last_sql
        assert b2.pub_date is None


async def test_reading(flushdb):
    u1 = await User.create(username='u1')
    u2 = await User.create(username='u2')

    assert u1 == await User.get(username='u1')
    assert u2 == await User.get(username='u2')
    assert u1 != u2

    assert u1 == await User.get(User.username == 'u1')
    assert u2 == await User.get(User.username == 'u2')


async def test_get_exception(flushdb):
    exc = None
    try:
        await User.get(User.id == 0)
    except Exception as raised_exc:
        exc = raised_exc
    else:
        assert False

    assert exc.__module__ == 'models'
    assert str(type(exc)) == "<class 'models.UserDoesNotExist'>"


async def test_get_or_create(flushdb):
    u1, created = await User.get_or_create(username='u1')
    assert created is True

    u1_x, created = await User.get_or_create(username='u1')
    assert created is False

    assert u1.id == u1_x.id
    assert await User.select().count() == 1


async def test_get_or_create_extended(flushdb):
    gc1, created = await GCModel.get_or_create(
        name='huey',
        key='k1',
        value='v1',
        defaults={'number': 3})

    assert created is True
    assert gc1.name == 'huey'
    assert gc1.key == 'k1'
    assert gc1.value == 'v1'
    assert gc1.number == 3

    gc1_db, created = await GCModel.get_or_create(
        name='huey',
        defaults={'key': 'k2', 'value': 'v2'})
    assert created is False
    assert gc1_db.id == gc1.id
    assert gc1_db.key == 'k1'

    with pytest.raises(IntegrityError):
        gc2, created = await GCModel.get_or_create(
            name='huey',
            key='kx',
            value='vx')

    gc2, created = await GCModel.get_or_create(
        name__ilike='%nugget%',
        defaults={'name': 'foo-nugget',
                  'key': 'k2',
                  'value': 'v2'})
    assert created is True
    assert gc2.name == 'foo-nugget'

    gc2_db, created = await GCModel.get_or_create(
        name__ilike='%nugg%',
        defaults={'name': 'xx'})
    assert created is False
    assert gc2_db.id == gc2.id

    assert await GCModel.select().count() == 2


async def test_peek(flushdb):
    users = await User.create_users(3)

    with assert_query_count(1):
        sq = User.select().order_by(User.username)

        # call it once
        u1 = await sq.peek()
        assert u1.username == 'u1'

        # check the result cache
        assert len(sq._qr._result_cache) == 1

        # call it again and we get the same result, but not an
        # extra query
        assert (await sq.peek()).username == 'u1'

    with assert_query_count(0):
        # no limit is applied.
        usernames = [u.username async for u in sq]
        assert usernames == ['u1', 'u2', 'u3']


async def test_first(flushdb):
    users = await User.create_users(3)

    with assert_query_count(1):
        sq = User.select().order_by(User.username)

        # call it once
        first = await sq.first()
        assert first.username == 'u1'

        # check the result cache
        assert len(sq._qr._result_cache) == 1

        # call it again and we get the same result, but not an
        # extra query
        assert (await sq.first()).username == 'u1'

    with assert_query_count(0):
        # also note that a limit has been applied.
        all_results = [obj async for obj in sq]
        assert all_results == [first]

        usernames = [u.username async for u in sq]
        assert usernames == ['u1']

    with assert_query_count(0):
        # call first() after iterating
        assert (await sq.first()).username == 'u1'

        usernames = [u.username async for u in sq]
        assert usernames == ['u1']

    # call it with an empty result
    sq = User.select().where(User.username == 'not-here')
    assert await sq.first() is None


async def test_deleting(flushdb):
    u1 = await User.create(username='u1')
    u2 = await User.create(username='u2')

    assert await User.select().count() == 2
    await u1.delete_instance()
    assert await User.select().count() == 1

    assert u2 == await User.get(User.username=='u2')


async def test_counting(flushdb):
    u1 = await User.create(username='u1')
    u2 = await User.create(username='u2')

    for u in [u1, u2]:
        for i in range(5):
            await Blog.create(title=f'b-{u.username}-{i}', user=u)

    uc = User.select().where(User.username == 'u1').join(Blog).count()
    assert await uc == 5

    uc = User.select().where(User.username == 'u1').join(Blog).distinct().count()
    assert await uc == 1

    assert await Blog.select().limit(4).offset(3).count() == 4
    assert await Blog.select().limit(4).offset(3).count(True) == 10

    # Calling `distinct()` will result in a call to wrapped_count().
    uc = User.select().join(Blog).distinct().count()
    assert await uc == 2

    # Test with clear limit = True.
    assert await User.select().limit(1).count(clear_limit=True) == 2
    assert await User.select().limit(1).wrapped_count(clear_limit=True) == 2

    # Test with clear limit = False.
    assert await User.select().limit(1).count(clear_limit=False) == 1
    assert await User.select().limit(1).wrapped_count(clear_limit=False) == 1


async def test_ordering(flushdb):
    u1 = await User.create(username='u1')
    u2 = await User.create(username='u2')
    u3 = await User.create(username='u2')
    users = User.select().order_by(User.username.desc(), User.id.desc())
    assert [u._get_pk_value() async for u in users] == [u3.id, u2.id, u1.id]


# async def test_count_transaction(flushdb):
#     for i in range(10):
#         await User.create(username='u%d' % i)

#     async with db.transaction():
#         async for user in User.select():
#             for i in range(20):
#                 await Blog.create(user=user, title='b-%d-%d' % (user.id, i))

#     count = Blog.select().count()
#     assert count == 200


async def test_exists(flushdb):
    u1 = await User.create(username='u1')
    assert await User.select().where(User.username == 'u1').exists() is True
    assert await User.select().where(User.username == 'u2').exists() is False


async def test_unicode(flushdb):
    # create a unicode literal
    ustr = 'Lýðveldið Ísland'
    u = await User.create(username=ustr)

    # query using the unicode literal
    u_db = await User.get(User.username == ustr)

    # the db returns a unicode literal
    assert u_db.username == ustr

    # delete the user
    assert await u.delete_instance() == 1

    # convert the unicode to a utf8 string
    utf8_str = ustr.encode('utf-8')

    # create using the utf8 string
    u2 = await User.create(username=utf8_str)

    # query using unicode literal
    u2_db = await User.get(User.username == ustr)

    # we get unicode back
    assert u2_db.username == ustr


async def test_unicode_issue202(flushdb):
    ustr = 'M\u00f6rk'
    user = await User.create(username=ustr)
    assert user.username == ustr


# async def test_on_conflict(flushdb):
#     gc = await GCModel.create(name='g1', key='k1', value='v1')
#     query = GCModel.insert(
#         name='g1',
#         key='k2',
#         value='v2')
#     with pytest.raises(IntegrityError):
#         await query.execute()

#     # Ensure that we can ignore errors.
#     res = await query.on_conflict('IGNORE').execute()
#     assert res == gc.id
#     assert await GCModel.select().count() == 1

#     # Error ignored, no changes.
#     gc_db = await GCModel.get()
#     assert gc_db.name == 'g1'
#     assert gc_db.key == 'k1'
#     assert gc_db.value == 'v1'

#     # Replace the old, conflicting row, with the new data.
#     res = await query.on_conflict('REPLACE').execute()
#     assert res != gc.id
#     assert await GCModel.select().count() == 1

#     gc_db = await GCModel.get()
#     assert gc_db.name == 'g1'
#     assert gc_db.key == 'k2'
#     assert gc_db.value == 'v2'

#     # Replaces also can occur when violating multi-column indexes.
#     query = GCModel.insert(
#         name='g2',
#         key='k2',
#         value='v2').on_conflict('REPLACE')

#     res = await query.execute()
#     assert res != gc_db.id
#     assert await GCModel.select().count() == 1

#     gc_db = await GCModel.get()
#     assert gc_db.name == 'g2'
#     assert gc_db.key == 'k2'
#     assert gc_db.value == 'v2'


#     def test_on_conflict_many(self):
#         if not SqliteDatabase.insert_many:
#             return

#         for i in range(5):
#             key = 'gc%s' % i
#             GCModel.create(name=key, key=key, value=key)

#         insert = [
#             {'name': key, 'key': 'x-%s' % key, 'value': key}
#             for key in ['gc%s' % i for i in range(10)]]
#         res = GCModel.insert_many(insert).on_conflict('IGNORE').execute()
#         assert GCModel.select().count(), 10)

#         gcs = list(GCModel.select().order_by(GCModel.id))
#         first_five, last_five = gcs[:5], gcs[5:]

#         # The first five should all be "gcI", the last five will have
#         # "x-gcI" for their keys.
#         assert
#             [gc.key for gc in first_five],
#             ['gc0', 'gc1', 'gc2', 'gc3', 'gc4'])

#         assert
#             [gc.key for gc in last_five],
#             ['x-gc5', 'x-gc6', 'x-gc7', 'x-gc8', 'x-gc9'])


async def test_meta_get_field_index():
    index = Blog._meta.get_field_index(Blog.content)
    assert index == 3


async def test_meta_remove_field():

    class _Model(Model):
        title = CharField(max_length=25)
        content = TextField(default='')

    _Model._meta.remove_field('content')
    assert 'content' not in _Model._meta.fields
    assert 'content' not in _Model._meta.sorted_field_names
    assert [f.name for f in _Model._meta.sorted_fields] == ['id', 'title']


async def test_meta_rel_for_model():
    class User(Model):
        pass
    class Category(Model):
        parent = ForeignKeyField('self')
    class Tweet(Model):
        user = ForeignKeyField(User)
    class Relationship(Model):
        from_user = ForeignKeyField(User, related_name='r1')
        to_user = ForeignKeyField(User, related_name='r2')

    UM = User._meta
    CM = Category._meta
    TM = Tweet._meta
    RM = Relationship._meta

    # Simple refs work.
    assert UM.rel_for_model(Tweet) is None
    assert UM.rel_for_model(Tweet, multi=True) == []
    assert UM.reverse_rel_for_model(Tweet) == Tweet.user
    assert UM.reverse_rel_for_model(Tweet, multi=True) == [Tweet.user]

    # Multi fks.
    assert RM.rel_for_model(User) == Relationship.from_user
    assert RM.rel_for_model(User, multi=True) == [Relationship.from_user,
                                                  Relationship.to_user]

    assert UM.reverse_rel_for_model(Relationship) == Relationship.from_user

    exp = [Relationship.from_user, Relationship.to_user]
    assert UM.reverse_rel_for_model(Relationship, multi=True) == exp

    # Self-refs work.
    assert CM.rel_for_model(Category) == Category.parent
    assert CM.reverse_rel_for_model(Category) == Category.parent

    # Field aliases work.
    UA = User.alias()
    assert TM.rel_for_model(UA) == Tweet.user


async def create_ordered_models():
    return [await OrderedModel.create(
            title=i, created=datetime.datetime(2013, 1, i + 1))
            for i in range(3)]


async def create_user_blogs():
    users = []
    ct = 0
    for i in range(2):
        user = await User.create(username='u-%d' % i)
        for j in range(2):
            ct += 1
            await Blog.create(
                user=user,
                title='b-%d-%d' % (i, j),
                pub_date=datetime.datetime(2013, 1, ct))
        users.append(user)
    return users


async def test_annotate_int(flushdb):
    users = await create_user_blogs()
    annotated = await User.select().annotate(
        Blog, fn.Count(Blog.pk).alias('ct'))

    for i, user in enumerate(annotated):
        assert user.ct == 2
        assert user.username == 'u-%d' % i


async def test_annotate_datetime(flushdb):
    users = await create_user_blogs()
    annotated = await (User.select()
                           .annotate(Blog, fn.Max(Blog.pub_date).alias('max_pub')))
    user_0, user_1 = annotated
    assert user_0.max_pub == datetime.datetime(2013, 1, 2)
    assert user_1.max_pub == datetime.datetime(2013, 1, 4)


async def test_aggregate_int(flushdb):
    models = await create_ordered_models()
    max_id = await OrderedModel.select().aggregate(fn.Max(OrderedModel.id))
    assert max_id == models[-1].id


async def test_aggregate_datetime(flushdb):
    models = await create_ordered_models()
    max_created = await (OrderedModel.select()
                                     .aggregate(fn.Max(OrderedModel.created)))
    assert max_created == models[-1].created


# class TestMultiTableFromClause(ModelTestCase):
#     requires = [Blog, Comment, User]


#     def test_subselect(self):
#         inner = User.select(User.username)
#         assert
#             [u.username for u in inner.order_by(User.username)], ['u0', 'u1'])

#         # Have to manually specify the alias as "t1" because the outer query
#         # will expect that.
#         outer = (User
#                  .select(User.username)
#                  .from_(inner.alias('t1')))
#         sql, params = compiler.generate_select(outer)
#         assert sql, (
#             'SELECT "users"."username" FROM '
#             '(SELECT "users"."username" FROM "users" AS users) AS t1'))

#         assert
#             [u.username for u in outer.order_by(User.username)], ['u0', 'u1'])

#     def test_subselect_with_column(self):
#         inner = User.select(User.username.alias('name')).alias('t1')
#         outer = (User
#                  .select(inner.c.name)
#                  .from_(inner))
#         sql, params = compiler.generate_select(outer)
#         assert sql, (
#             'SELECT "t1"."name" FROM '
#             '(SELECT "users"."username" AS name FROM "users" AS users) AS t1'))

#         query = outer.order_by(inner.c.name.desc())
#         assert [u[0] for u in query.tuples()], ['u1', 'u0'])


async def create_users_blogs_comments():
    for u in range(2):
        user = await User.create(username='u%s' % u)
        for i in range(3):
            b = await Blog.create(user=user, title='b%s-%s' % (u, i))
            for j in range(i):
                await Comment.create(blog=b, comment='c%s-%s' % (i, j))


async def test_from_multi_table(flushdb):
    await create_users_blogs_comments()

    q = (Blog
         .select(Blog, User)
         .from_(Blog, User)
         .where(
             (Blog.user == User.id) &
             (User.username == 'u0'))
         .order_by(Blog.pk)
         .naive())

    with assert_query_count(1):
        blogs = [b.title async for b in q]
        assert blogs == ['b0-0', 'b0-1', 'b0-2']

        # TODO: query iterator can't be restarted
        # usernames = [b.username async for b in q]
        # assert usernames == ['u0', 'u0', 'u0']



# async def test_subselect_with_join():
#     inner = User.select(User.id, User.username).alias('q1')
#     outer = (Blog
#              .select(inner.c.id, inner.c.username)
#              .from_(inner)
#              .join(Comment, on=(inner.c.id == Comment.id)))
#     sql, params = compiler.generate_select(outer)
#     assert sql, (
#         'SELECT "q1"."id", "q1"."username" FROM ('
#         'SELECT "users"."id", "users"."username" FROM "users" AS users) AS q1 '
#         'INNER JOIN "comment" AS comment ON ("q1"."id" = "comment"."id")'))


async def test_join_on_query(flushdb):
    await create_users_blogs_comments()

    u0 = await User.get(User.username == 'u0')
    u1 = await User.get(User.username == 'u1')

    inner = User.select().alias('j1')
    outer = (Blog
             .select(Blog.title, Blog.user)
             .join(inner, on=(Blog.user == inner.c.id))
             .order_by(Blog.pk))
    res = [row async for row in outer.tuples()]
    assert res == [
        ('b0-0', u0.id),
        ('b0-1', u0.id),
        ('b0-2', u0.id),
        ('b1-0', u1.id),
        ('b1-1', u1.id),
        ('b1-2', u1.id),
    ]


# class TestDeleteRecursive(ModelTestCase):
#     requires = [
#         Parent, Child, ChildNullableData, ChildPet, Orphan, OrphanPet, Package,
#         PackageItem]

#     def setUp(self):
#         super(TestDeleteRecursive, self).setUp()
#         self.p1 = p1 = Parent.create(data='p1')
#         self.p2 = p2 = Parent.create(data='p2')
#         c11 = Child.create(parent=p1)
#         c12 = Child.create(parent=p1)
#         c21 = Child.create(parent=p2)
#         c22 = Child.create(parent=p2)
#         o11 = Orphan.create(parent=p1)
#         o12 = Orphan.create(parent=p1)
#         o21 = Orphan.create(parent=p2)
#         o22 = Orphan.create(parent=p2)

#         for child in [c11, c12, c21, c22]:
#             ChildPet.create(child=child)

#         for orphan in [o11, o12, o21, o22]:
#             OrphanPet.create(orphan=orphan)

#         for i, child in enumerate([c11, c12]):
#             for j in range(2):
#                 ChildNullableData.create(
#                     child=child,
#                     data='%s-%s' % (i, j))

#     def test_recursive_delete_parent_sql(self):
#         with self.log_queries() as query_logger:
#             with assert_query_count(5):
#                 self.p1.delete_instance(recursive=True, delete_nullable=False)

#         queries = query_logger.queries
#         update_cnd = ('UPDATE `childnullabledata` '
#                       'SET `child_id` = %% '
#                       'WHERE ('
#                       '`childnullabledata`.`child_id` IN ('
#                       'SELECT `t2`.`id` FROM `child` AS t2 WHERE ('
#                       '`t2`.`parent_id` = %%)))')
#         delete_cp = ('DELETE FROM `childpet` WHERE ('
#                      '`child_id` IN ('
#                      'SELECT `t1`.`id` FROM `child` AS t1 WHERE ('
#                      '`t1`.`parent_id` = %%)))')
#         delete_c = 'DELETE FROM `child` WHERE (`parent_id` = %%)'
#         update_o = ('UPDATE `orphan` SET `parent_id` = %% WHERE ('
#                     '`orphan`.`parent_id` = %%)')
#         delete_p = 'DELETE FROM `parent` WHERE (`id` = %%)'
#         sql_params = [
#             (update_cnd, [None, self.p1.id]),
#             (delete_cp, [self.p1.id]),
#             (delete_c, [self.p1.id]),
#             (update_o, [None, self.p1.id]),
#             (delete_p, [self.p1.id]),
#         ]
#         self.assertQueriesEqual(queries, sql_params)

#     def test_recursive_delete_child_queries(self):
#         c2 = self.p1.child_set.order_by(Child.id.desc()).get()
#         with self.log_queries() as query_logger:
#             with assert_query_count(3):
#                 c2.delete_instance(recursive=True, delete_nullable=False)

#         queries = query_logger.queries

#         update_cnd = ('UPDATE `childnullabledata` SET `child_id` = %% WHERE ('
#                       '`childnullabledata`.`child_id` = %%)')
#         delete_cp = 'DELETE FROM `childpet` WHERE (`child_id` = %%)'
#         delete_c = 'DELETE FROM `child` WHERE (`id` = %%)'

#         sql_params = [
#             (update_cnd, [None, c2.id]),
#             (delete_cp, [c2.id]),
#             (delete_c, [c2.id]),
#         ]
#         self.assertQueriesEqual(queries, sql_params)


#     def test_recursive_update(self):
#         self.p1.delete_instance(recursive=True)
#         counts = (
#             #query,fk,p1,p2,tot
#             (Child.select(), Child.parent, 0, 2, 2),
#             (Orphan.select(), Orphan.parent, 0, 2, 4),
#             (ChildPet.select().join(Child), Child.parent, 0, 2, 2),
#             (OrphanPet.select().join(Orphan), Orphan.parent, 0, 2, 4),
#         )

#         for query, fk, p1_ct, p2_ct, tot in counts:
#             assert query.where(fk == self.p1).count(), p1_ct)
#             assert query.where(fk == self.p2).count(), p2_ct)
#             assert query.count(), tot)

#     def test_recursive_delete(self):
#         self.p1.delete_instance(recursive=True, delete_nullable=True)
#         counts = (
#             #query,fk,p1,p2,tot
#             (Child.select(), Child.parent, 0, 2, 2),
#             (Orphan.select(), Orphan.parent, 0, 2, 2),
#             (ChildPet.select().join(Child), Child.parent, 0, 2, 2),
#             (OrphanPet.select().join(Orphan), Orphan.parent, 0, 2, 2),
#         )

#         for query, fk, p1_ct, p2_ct, tot in counts:
#             assert query.where(fk == self.p1).count(), p1_ct)
#             assert query.where(fk == self.p2).count(), p2_ct)
#             assert query.count(), tot)

#     def test_recursive_non_pk_fk(self):
#         for i in range(3):
#             Package.create(barcode=str(i))
#             for j in range(4):
#                 PackageItem.create(package=str(i), title='%s-%s' % (i, j))

#         assert Package.select().count(), 3)
#         assert PackageItem.select().count(), 12)

#         Package.get(Package.barcode == '1').delete_instance(recursive=True)

#         assert Package.select().count(), 2)
#         assert PackageItem.select().count(), 8)

#         items = (PackageItem
#                  .select(PackageItem.title)
#                  .order_by(PackageItem.id)
#                  .tuples())
#         assert [i[0] for i in items], [
#             '0-0', '0-1', '0-2', '0-3',
#             '2-0', '2-1', '2-2', '2-3',
#         ])


# skip if test db is mysqldatabase
# async def test_truncate(flushdb):
#     for i in range(3):
#         await User.create(username='u%s' % i)

#     await User.truncate_table(restart_identity=True)
#     assert await User.select().count() == 0

#     u = await User.create(username='ux')
#     assert u.id == 1


async def create_many_to_many():
    users = ['u1', 'u2', 'u3']
    categories = ['c1', 'c2', 'c3', 'c12', 'c23']
    user_to_cat = {
        'u1': ['c1', 'c12'],
        'u2': ['c2', 'c12', 'c23'],
    }
    for u in users:
        await User.create(username=u)
    for c in categories:
        await Category.create(name=c)
    for user, categories in user_to_cat.items():
        user = await User.get(User.username == user)
        for category in categories:
            category = await Category.get(Category.name == category)
            await UserCategory.create(user=user, category=category)


async def test_m2m(flushdb):
    await create_many_to_many()

    async def aU(q, exp):
        assert [u.username async for u in q.order_by(User.username)] == exp

    async def aC(q, exp):
        assert [c.name async for c in q.order_by(Category.name)] == exp

    users = (User.select()
                 .join(UserCategory)
                 .join(Category)
                 .where(Category.name == 'c1'))
    await aU(users, ['u1'])

    users = (User.select()
                 .join(UserCategory)
                 .join(Category)
                 .where(Category.name == 'c3'))
    await aU(users, [])

    cats = (Category.select()
                    .join(UserCategory)
                    .join(User)
                    .where(User.username == 'u1'))
    await aC(cats, ['c1', 'c12'])

    cats = (Category.select()
                    .join(UserCategory)
                    .join(User)
                    .where(User.username == 'u2'))
    await aC(cats, ['c12', 'c2', 'c23'])

    cats = (Category.select()
                    .join(UserCategory)
                    .join(User)
                    .where(User.username == 'u3'))
    await aC(cats, [])

    cats = (Category.select()
                    .join(UserCategory)
                    .join(User)
                    .where(Category.name << ['c1', 'c2', 'c3']))
    await aC(cats, ['c1', 'c2'])

    cats = (Category.select()
                    .join(UserCategory, JOIN.LEFT_OUTER)
                    .join(User, JOIN.LEFT_OUTER)
                    .where(Category.name << ['c1', 'c2', 'c3']))
    await aC(cats, ['c1', 'c2', 'c3'])


# async def test_many_to_many_prefetch(flushdb):
#     await create_many_to_many()

#     categories = Category.select().order_by(Category.name)
#     user_categories = UserCategory.select().order_by(UserCategory.id)
#     users = User.select().order_by(User.username)
#     results = {}
#     result_list = []
#     with assert_query_count(3):
#         query = prefetch(categories, user_categories, users)
#         for category in query:
#             results.setdefault(category.name, set())
#             result_list.append(category.name)
#             for user_category in category.usercategory_set_prefetch:
#                 results[category.name].add(user_category.user.username)
#                 result_list.append(user_category.user.username)

#     assert results, {
#         'c1': set(['u1']),
#         'c12': set(['u1', 'u2']),
#         'c2': set(['u2']),
#         'c23': set(['u2']),
#         'c3': set(),
#     })
#     assert
#         sorted(result_list),
#         ['c1', 'c12', 'c2', 'c23', 'c3', 'u1', 'u1', 'u2', 'u2', 'u2'])


async def test_custom_model_options_base(database):
    db = database

    class DatabaseDescriptor(object):
        def __init__(self, db):
            self._db = db

        def __get__(self, instance_type, instance):
            if instance is not None:
                return self._db
            return self

        def __set__(self, instance, value):
            pass

    class TestModelOptions(ModelOptions):
        database = DatabaseDescriptor(db)

    class BaseModel(Model):
        class Meta:
            model_options_base = TestModelOptions

    class TestModel(BaseModel):
        pass

    class TestChildModel(TestModel):
        pass

    assert id(TestModel._meta.database) == id(db)
    assert id(TestChildModel._meta.database) == id(db)


async def test_db_table(flushdb):
    assert User._meta.db_table == 'users'

    class Foo(TestModel):
        pass
    assert Foo._meta.db_table == 'foo'

    class Foo2(TestModel):
        pass
    assert Foo2._meta.db_table == 'foo2'

    class Foo_3(TestModel):
        pass
    assert Foo_3._meta.db_table == 'foo_3'


async def test_custom_options():
    class A(Model):
        class Meta:
            a = 'a'

    class B1(A):
        class Meta:
            b = 1

    class B2(A):
        class Meta:
            b = 2

    assert A._meta.a == 'a'
    assert B1._meta.a == 'a'
    assert B2._meta.a == 'a'
    assert B1._meta.b == 1
    assert B2._meta.b == 2


# def test_option_inheritance(self):
#     x_test_db = SqliteDatabase('testing.db')
#     child2_db = SqliteDatabase('child2.db')

#     class FakeUser(Model):
#         pass

#     class ParentModel(Model):
#         title = CharField()
#         user = ForeignKeyField(FakeUser)

#         class Meta:
#             database = x_test_db

#     class ChildModel(ParentModel):
#         pass

#     class ChildModel2(ParentModel):
#         special_field = CharField()

#         class Meta:
#             database = child2_db

#     class GrandChildModel(ChildModel):
#         pass

#     class GrandChildModel2(ChildModel2):
#         special_field = TextField()

#     assert ParentModel._meta.database.database, 'testing.db')
#     assert ParentModel._meta.model_class, ParentModel)

#     assert ChildModel._meta.database.database, 'testing.db')
#     assert ChildModel._meta.model_class, ChildModel)
#     assert sorted(ChildModel._meta.fields.keys()), [
#         'id', 'title', 'user'
#     ])

#     assert ChildModel2._meta.database.database, 'child2.db')
#     assert ChildModel2._meta.model_class, ChildModel2)
#     assert sorted(ChildModel2._meta.fields.keys()), [
#         'id', 'special_field', 'title', 'user'
#     ])

#     assert GrandChildModel._meta.database.database, 'testing.db')
#     assert GrandChildModel._meta.model_class, GrandChildModel)
#     assert sorted(GrandChildModel._meta.fields.keys()), [
#         'id', 'title', 'user'
#     ])

#     assert GrandChildModel2._meta.database.database, 'child2.db')
#     assert GrandChildModel2._meta.model_class, GrandChildModel2)
#     assert sorted(GrandChildModel2._meta.fields.keys()), [
#         'id', 'special_field', 'title', 'user'
#     ])
#     self.assertTrue(isinstance(GrandChildModel2._meta.fields['special_field'], TextField))


async def test_order_by_inheritance():
    class Base(TestModel):
        created = DateTimeField()

        class Meta:
            order_by = ('-created',)

    class Foo(Base):
        data = CharField()

    class Bar(Base):
        val = IntegerField()
        class Meta:
            order_by = ('-val',)

    foo_order_by = Foo._meta.order_by[0]
    assert isinstance(foo_order_by, Field)
    assert foo_order_by.model_class is Foo
    assert foo_order_by.name == 'created'

    bar_order_by = Bar._meta.order_by[0]
    assert isinstance(bar_order_by, Field)
    assert bar_order_by.model_class is Bar
    assert bar_order_by.name == 'val'


async def test_table_name_function():
    class Base(TestModel):
        class Meta:
            def db_table_func(model):
                return model.__name__.lower() + 's'

    class User(Base):
        pass

    class SuperUser(User):
        class Meta:
            db_table = 'nugget'

    class MegaUser(SuperUser):
        class Meta:
            def db_table_func(model):
                return 'mega'

    class Bear(Base):
        pass

    assert User._meta.db_table == 'users'
    assert Bear._meta.db_table == 'bears'
    assert SuperUser._meta.db_table == 'nugget'
    assert MegaUser._meta.db_table == 'mega'


async def test_model_inheritance_attrs():
    exp = ['pk', 'user', 'title', 'content', 'pub_date']
    assert Blog._meta.sorted_field_names == exp

    exp = ['pk', 'user', 'content', 'pub_date', 'title', 'extra_field']
    assert BlogTwo._meta.sorted_field_names == exp

    assert Blog._meta.primary_key.name == 'pk'
    assert BlogTwo._meta.primary_key.name == 'pk'

    assert Blog.user.related_name == 'blog_set'
    assert BlogTwo.user.related_name == 'blogtwo_set'

    assert User.blog_set.rel_model == Blog
    assert User.blogtwo_set.rel_model == BlogTwo

    assert BlogTwo._meta.db_table != Blog._meta.db_table


async def test_model_inheritance_flow(flushdb):
    u = await User.create(username='u')

    b = await Blog.create(title='b', user=u)
    b2 = await BlogTwo.create(title='b2', extra_field='foo', user=u)

    assert await alist(u.blog_set) == [b]
    assert await alist(u.blogtwo_set) == [b2]

    assert await Blog.select().count() == 1
    assert await BlogTwo.select().count() == 1

    b_from_db = await Blog.get(Blog.pk == b.pk)
    b2_from_db = await BlogTwo.get(BlogTwo.pk == b2.pk)

    assert await b_from_db.user == u
    assert await b2_from_db.user == u
    assert b2_from_db.extra_field == 'foo'


# async def test_inheritance_primary_keys(flushdb):
#     assert hasattr(Model, 'id')

#     class M1(Model): pass
#     assert hasattr(M1, 'id')

#     class M2(Model):
#         key = CharField(primary_key=True)
#     assert not hasattr(M2, 'id')

#     class M3(Model):
#         id = TextField()
#         key = IntegerField(primary_key=True)
#     assert hasattr(M3, 'id')
#     assert not M3.id.primary_key

#     class C1(M1): pass
#     assert hasattr(C1, 'id')
#     assert C1.id.model_class is C1

#     class C2(M2): pass
#     assert not hasattr(C2, 'id')
#     assert C2.key.primary_key
#     assert C2.key.model_class is C2

#     class C3(M3): pass
#     assert hasattr(C3, 'id')
#     assert not C3.id.primary_key
#     assert C3.id.model_class is C3


# class TestAliasBehavior(ModelTestCase):
#     requires = [UpperModel]

#     def test_alias_with_coerce(self):
#         UpperModel.create(data='test')
#         um = UpperModel.get()
#         assert um.data, 'TEST')

#         Alias = UpperModel.alias()
#         normal = (UpperModel.data == 'foo')
#         aliased = (Alias.data == 'foo')
#         _, normal_p = compiler.parse_node(normal)
#         _, aliased_p = compiler.parse_node(aliased)
#         assert normal_p, ['FOO'])
#         assert aliased_p, ['FOO'])

#         expected = (
#             'SELECT "uppermodel"."id", "uppermodel"."data" '
#             'FROM "uppermodel" AS uppermodel '
#             'WHERE ("uppermodel"."data" = ?)')

#         query = UpperModel.select().where(UpperModel.data == 'foo')
#         sql, params = compiler.generate_select(query)
#         assert sql, expected)
#         assert params, ['FOO'])

#         query = Alias.select().where(Alias.data == 'foo')
#         sql, params = compiler.generate_select(query)
#         assert sql, expected)
#         assert params, ['FOO'])


# @skip_unless(lambda: isinstance(test_db, PostgresqlDatabase))
# class TestInsertReturningModelAPI(PeeweeTestCase):
#     def setUp(self):
#         super(TestInsertReturningModelAPI, self).setUp()

#         self.db = database_initializer.get_database(
#             'postgres',
#             PostgresqlDatabase)

#         class BaseModel(TestModel):
#             class Meta:
#                 database = self.db

#         self.BaseModel = BaseModel
#         self.models = []

#     def tearDown(self):
#         if self.models:
#             self.db.drop_tables(self.models, True)
#         super(TestInsertReturningModelAPI, self).tearDown()

#     def test_insert_returning(self):
#         class User(self.BaseModel):
#             username = CharField()
#             class Meta:
#                 db_table = 'users'

#         self.models.append(User)
#         User.create_table()

#         query = User.insert(username='charlie')
#         sql, params = query.sql()
#         assert sql, (
#             'INSERT INTO "users" ("username") VALUES (%s) RETURNING "id"'))
#         assert params, ['charlie'])

#         result = query.execute()
#         charlie = User.get(User.username == 'charlie')
#         assert result, charlie.id)

#         result2 = User.insert(username='huey').execute()
#         self.assertTrue(result2 > result)
#         huey = User.get(User.username == 'huey')
#         assert result2, huey.id)

#         mickey = User.create(username='mickey')
#         assert mickey.id, huey.id + 1)
#         mickey.save()
#         assert User.select().count(), 3)

#     def test_non_int_pk(self):
#         class User(self.BaseModel):
#             username = CharField(primary_key=True)
#             data = IntegerField()
#             class Meta:
#                 db_table = 'users'

#         self.models.append(User)
#         User.create_table()

#         query = User.insert(username='charlie', data=1337)
#         sql, params = query.sql()
#         assert sql, (
#             'INSERT INTO "users" ("username", "data") '
#             'VALUES (%s, %s) RETURNING "username"'))
#         assert params, ['charlie', 1337])

#         assert query.execute(), 'charlie')
#         charlie = User.get(User.data == 1337)
#         assert charlie.username, 'charlie')

#         huey = User.create(username='huey', data=1024)
#         assert huey.username, 'huey')
#         assert huey.data, 1024)

#         huey_db = User.get(User.data == 1024)
#         assert huey_db.username, 'huey')
#         huey_db.save()
#         assert huey_db.username, 'huey')

#         assert User.select().count(), 2)

#     def test_composite_key(self):
#         class Person(self.BaseModel):
#             first = CharField()
#             last = CharField()
#             data = IntegerField()

#             class Meta:
#                 primary_key = CompositeKey('first', 'last')

#         self.models.append(Person)
#         Person.create_table()

#         query = Person.insert(first='huey', last='leifer', data=3)
#         sql, params = query.sql()
#         assert sql, (
#             'INSERT INTO "person" ("first", "last", "data") '
#             'VALUES (%s, %s, %s) RETURNING "first", "last"'))
#         assert params, ['huey', 'leifer', 3])

#         res = query.execute()
#         assert res, ['huey', 'leifer'])

#         huey = Person.get(Person.data == 3)
#         assert huey.first, 'huey')
#         assert huey.last, 'leifer')

#         zaizee = Person.create(first='zaizee', last='owen', data=2)
#         assert zaizee.first, 'zaizee')
#         assert zaizee.last, 'owen')

#         z_db = Person.get(Person.data == 2)
#         assert z_db.first, 'zaizee')
#         assert z_db.last, 'owen')
#         z_db.save()

#         assert Person.select().count(), 2)

# async def test_insert_many(flushdb):
#     class User(Model):
#         username = CharField()
#         class Meta:
#             database = db
#             db_table = 'users'

#     await db.create_tables([User], safe=True)

#     usernames = ['charlie', 'huey', 'zaizee']
#     data = [{'username': username} for username in usernames]

#     query = User.insert_many(data)
#     sql, params = query.sql()
#     assert sql == ('INSERT INTO "users" ("username") '
#                    'VALUES (%s), (%s), (%s)')
#     assert params == usernames

#     res = await query.execute()
#     assert res is True
#     assert await User.select().count() == 3
#     z = await User.select().order_by(-User.username).get()
#     assert z.username == 'zaizee'

#     usernames = ['foo', 'bar', 'baz']
#     data = [{'username': username} for username in usernames]
#     query = User.insert_many(data).return_id_list()
#     sql, params = query.sql()
#     assert sql == ('INSERT INTO "users" ("username") '
#                    'VALUES (%s), (%s), (%s) RETURNING "id"')
#     assert params == usernames

#     res = list(await query.execute())
#     assert len(res) == 3
#     foo = await User.get(User.username == 'foo')
#     bar = await User.get(User.username == 'bar')
#     baz = await User.get(User.username == 'baz')
#     assert res, [foo.id, bar.id, baz.id]

#     await User.drop_table()


# @skip_unless(lambda: isinstance(test_db, PostgresqlDatabase))
# class TestReturningClause(ModelTestCase):
#     requires = [User]

#     def test_update_returning(self):
#         User.create_users(3)
#         u1, u2, u3 = [user for user in User.select().order_by(User.id)]

#         uq = User.update(username='uII').where(User.id == u2.id)
#         res = uq.execute()
#         assert res, 1)  # Number of rows modified.

#         uq = uq.returning(User.username)
#         users = [user for user in uq.execute()]
#         assert len(users), 1)
#         user, = users
#         assert user.username, 'uII')
#         self.assertIsNone(user.id)  # Was not explicitly selected.

#         uq = (User
#               .update(username='huey')
#               .where(User.username != 'uII')
#               .returning(User))
#         users = [user for user in uq.execute()]
#         assert len(users), 2)
#         self.assertTrue(all([user.username == 'huey' for user in users]))
#         self.assertTrue(all([user.id is not None for user in users]))

#         uq = uq.dicts().returning(User.username)
#         user_data = [data for data in uq.execute()]
#         assert
#             user_data,
#             [{'username': 'huey'}, {'username': 'huey'}])


# async def test_delete_returning(flushdb):
#     await User.create_users(10)

#     dq = User.delete().where(User.username << ['u9', 'u10'])
#     res = await dq.execute()
#     assert res == 2  # Number of rows modified.

#     dq = (User
#           .delete()
#           .where(User.username << ['u7', 'u8'])
#           .returning(User.username))
#     users = [user async for user in dq.execute()]
#     assert len(users) == 2

#     usernames = sorted([user.username for user in users])
#     assert usernames == ['u7', 'u8']

#     ids = [user.id for user in users]
#     assert ids == [None, None]  # Was not selected.

#     dq = (User
#           .delete()
#           .where(User.username == 'u1')
#           .returning(User))
#     users = [user async for user in dq.execute()]
#     assert len(users) == 1
#     user, = users
#     assert user.username == 'u1'
#     assert user.id is not None


#     def test_insert_returning(self):
#         iq = User.insert(username='zaizee').returning(User)
#         users = [user for user in iq.execute()]
#         assert len(users), 1)
#         user, = users
#         assert user.username, 'zaizee')
#         self.assertIsNotNone(user.id)

#         iq = (User
#               .insert_many([
#                   {'username': 'charlie'},
#                   {'username': 'huey'},
#                   {'username': 'connor'},
#                   {'username': 'leslie'},
#                   {'username': 'mickey'}])
#               .returning(User))
#         users = sorted([user for user in iq.tuples().execute()])

#         usernames = [username for _, username in users]
#         assert usernames, [
#             'charlie',
#             'huey',
#             'connor',
#             'leslie',
#             'mickey',
#         ])

#         id_charlie = users[0][0]
#         id_mickey = users[-1][0]
#         assert id_mickey - id_charlie, 4)


async def test_hash():
    class MyUser(User):
        pass

    d = {}
    u1 = User(id=1)
    u2 = User(id=2)
    u3 = User(id=3)
    m1 = MyUser(id=1)
    m2 = MyUser(id=2)
    m3 = MyUser(id=3)

    d[u1] = 'u1'
    d[u2] = 'u2'
    d[m1] = 'm1'
    d[m2] = 'm2'
    assert u1 in d
    assert u2 in d
    assert u3 not in d
    assert m1 in d
    assert m2 in d
    assert m3 not in d

    assert d[u1] == 'u1'
    assert d[u2] == 'u2'
    assert d[m1] == 'm1'
    assert d[m2] == 'm2'

    un = User()
    mn = MyUser()
    d[un] = 'un'
    d[mn] = 'mn'
    assert un in d  # Hash implementation.
    assert mn in d
    assert d[un] == 'un'
    assert d[mn] == 'mn'


async def test_delete_nullable(flushdb):
    u = await User.create(username='u')
    n = await Note.create(user=u, text='n')
    f = await Flag.create(label='f')
    nf1 = await NoteFlagNullable.create(note=n, flag=f)
    nf2 = await NoteFlagNullable.create(note=n, flag=None)
    nf3 = await NoteFlagNullable.create(note=None, flag=f)
    nf4 = await NoteFlagNullable.create(note=None, flag=None)

    assert await nf1.delete_instance() == 1
    assert await nf2.delete_instance() == 1
    assert await nf3.delete_instance() == 1
    assert await nf4.delete_instance() == 1


async def create_parent_orphan_child():
    p1 = await Parent.create(data='p1')
    p2 = await Parent.create(data='p2')
    for i in range(1, 3):
        await Child.create(parent=p1, data='child%s-p1' % i)
        await Child.create(parent=p2, data='child%s-p2' % i)
        await Orphan.create(parent=p1, data='orphan%s-p1' % i)

    await Orphan.create(data='orphan1-noparent')
    await Orphan.create(data='orphan2-noparent')


async def test_no_empty_instances(flushdb):
    await create_parent_orphan_child()

    with assert_query_count(1):
        query = (Orphan
                 .select(Orphan, Parent)
                 .join(Parent, JOIN.LEFT_OUTER)
                 .order_by(Orphan.id))
        res = [(orphan.data, orphan.parent is None) async for orphan in query]

    assert res == [
        ('orphan1-p1', False),
        ('orphan2-p1', False),
        ('orphan1-noparent', True),
        ('orphan2-noparent', True),
    ]


async def test_unselected_fk_pk(flushdb):
    await create_parent_orphan_child()

    with assert_query_count(1):
        query = (Orphan
                 .select(Orphan.data, Parent.data)
                 .join(Parent, JOIN.LEFT_OUTER)
                 .order_by(Orphan.id))
        res = [(orphan.data, orphan.parent is None) async for orphan in query]

    assert res == [
        ('orphan1-p1', False),
        ('orphan2-p1', False),
        ('orphan1-noparent', False),
        ('orphan2-noparent', False),
    ]


async def test_non_null_fk_unselected_fk(flushdb):
    await create_parent_orphan_child()

    with assert_query_count(1):
        query = (Child
                 .select(Child.data, Parent.data)
                 .join(Parent, JOIN.LEFT_OUTER)
                 .order_by(Child.id))
        res = [(child.data, child.parent is None) async for child in query]

    assert res == [
        ('child1-p1', False),
        ('child1-p2', False),
        ('child2-p1', False),
        ('child2-p2', False),
    ]

    res = [child.parent.data async for child in query]
    assert res == ['p1', 'p2', 'p1', 'p2']

    res = [(child._data['parent'], child.parent.id) async for child in query]
    assert res == [
        (None, None),
        (None, None),
        (None, None),
        (None, None),
    ]


async def test_default_dirty(flushdb):
    DM = DefaultsModel
    DM._meta.only_save_dirty = True

    dm = DM()
    await dm.save()

    assert dm.field == 1
    assert dm.control == 1

    dm_db = await DM.get((DM.field == 1) & (DM.control == 1))
    assert dm_db.field == 1
    assert dm_db.control == 1

    # No changes.
    assert not await dm_db.save()

    dm2 = await DM.create()
    assert dm2.field == 3  # One extra when fetched from DB.
    assert dm2.control == 1

    dm._meta.only_save_dirty = False

    dm3 = DM()
    assert dm3.field == 4
    assert dm3.control == 1
    await dm3.save()

    dm3_db = await DM.get(DM.id == dm3.id)
    assert dm3_db.field == 4


async def test_function_coerce(database):
    db = database

    class M1(Model):
        data = IntegerField()
        class Meta:
            database = db

    class M2(Model):
        id = IntegerField()
        class Meta:
            database = db

    await db.create_tables([M1, M2])

    for i in range(3):
        await M1.create(data=i)
        await M2.create(id=i + 1)

    qm1 = M1.select(fn.GROUP_CONCAT(M1.data).coerce(False).alias('data'))
    qm2 = M2.select(fn.GROUP_CONCAT(M2.id).coerce(False).alias('ids'))

    m1 = await qm1.get()
    assert m1.data == '0,1,2'

    m2 = await qm2.get()
    assert m2.ids == '1,2,3'

    await db.drop_tables([M1, M2])


# @skip_unless(
#     lambda: (isinstance(test_db, PostgresqlDatabase) or
#              (isinstance(test_db, SqliteDatabase) and supports_tuples)))
# class TestTupleComparison(ModelTestCase):
#     requires = [User]

#     def test_tuples(self):
#         ua = User.create(username='user-a')
#         ub = User.create(username='user-b')
#         uc = User.create(username='user-c')
#         query = User.select().where(
#             Tuple(User.username, User.id) == ('user-b', ub.id))
#         assert query.count(), 1)
#         obj = query.get()
#         assert obj, ub)


# requires recent peewee
# async def test_specify_object_id_name():
#     class User(Model): pass
#     class T0(Model):
#         user = ForeignKeyField(User)
#     class T1(Model):
#         user = ForeignKeyField(User, db_column='uid')
#     class T2(Model):
#         user = ForeignKeyField(User, object_id_name='uid')
#     class T3(Model):
#         user = ForeignKeyField(User, db_column='x', object_id_name='uid')
#     class T4(Model):
#         foo = ForeignKeyField(User, db_column='user')
#     class T5(Model):
#         foo = ForeignKeyField(User, object_id_name='uid')

#     user = User(id=1337)
#     assert T0(user=user).user_id == 1337
#     assert T1(user=user).uid == 1337
#     assert T2(user=user).uid == 1337
#     assert T3(user=user).uid == 1337
#     assert T4(foo=user).user == 1337
#     assert T5(foo=user).uid == 1337

#     with pytest.raises(ValueError):
#         class TE(Model):
#             user = ForeignKeyField(User, object_id_name='user')
