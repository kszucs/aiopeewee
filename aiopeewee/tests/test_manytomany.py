import pytest
from peewee import *
from models import db
from utils import assert_query_count
from playhouse.fields import DeferredThroughModel
from aiopeewee import AioModel, AioMySQLDatabase, AioManyToManyField as ManyToManyField


pytestmark = pytest.mark.asyncio


# from peewee import *

# from playhouse.fields import ManyToManyField
# from playhouse.tests.base import database_initializer
# from playhouse.tests.base import ModelTestCase
# db = database_initializer.get_in_memory_database()


class BaseModel(AioModel):
    class Meta:
        database = db


class User(BaseModel):
    username = CharField(unique=True)


class Note(BaseModel):
    text = TextField()
    users = ManyToManyField(User)


NoteUserThrough = Note.users.get_through_model()


AltThroughDeferred = DeferredThroughModel()


class AltNote(BaseModel):
    text = TextField()
    users = ManyToManyField(User, through_model=AltThroughDeferred)


class AltThroughModel(BaseModel):
    user = ForeignKeyField(User, related_name='_xx_rel')
    note = ForeignKeyField(AltNote, related_name='_xx_rel')

    class Meta:
        primary_key = CompositeKey('user', 'note')


AltThroughDeferred.set_model(AltThroughModel)


@pytest.yield_fixture
async def tables(database):
    tables = [User, Note, NoteUserThrough, AltThroughModel, AltNote]
    try:
        await database.create_tables(tables, safe=True)
        yield tables
    finally:
        await database.drop_tables(tables)


async def assert_notes(query, expected):
    notes = [note.text async for note in query]
    assert sorted(notes) == ['note-%s' % i for i in sorted(expected)]


async def assert_users(query, expected):
    usernames = [user.username async for user in query]
    assert sorted(usernames) == sorted(expected)


user_to_note = {'charlie': [1, 2],
                'huey': [2, 3],
                'mickey': [3, 4],
                'zaizee': [4, 5]}


async def create_user_notes():
    usernames = ['charlie', 'huey', 'mickey', 'zaizee']
    n_notes = 5
    for username in usernames:
        await User.create(username=username)
    for i in range(n_notes):
        await Note.create(text='note-%s' % (i + 1))


async def create_relationship():
    for username, notes in user_to_note.items():
        user = await User.get(User.username == username)
        for note in notes:
            await NoteUserThrough.create(
                note=await Note.get(Note.text == 'note-%s' % note),
                user=user)


async def test_through_model(tables):
    assert len(NoteUserThrough._meta.fields) == 3

    fields = NoteUserThrough._meta.fields
    assert sorted(fields) == ['id', 'note', 'user']

    note_field = fields['note']
    assert note_field.rel_model == Note
    assert note_field.null is False

    user_field = fields['user']
    assert user_field.rel_model == User
    assert user_field.null is False


async def test_descriptor_query(tables):
    await create_user_notes()
    await create_relationship()

    charlie, huey, mickey, zaizee = await User.select().order_by(User.username)

    with assert_query_count(1):
        await assert_notes(charlie.notes, [1, 2])

    with assert_query_count(1):
        await assert_notes(zaizee.notes, [4, 5])

    u = await User.create(username='beanie')
    await assert_notes(u.notes, [])

    n1, n2, n3, n4, n5 = await Note.select().order_by(Note.text)
    with assert_query_count(1):
        await assert_users(n1.users, ['charlie'])

    with assert_query_count(1):
        await assert_users(n2.users, ['charlie', 'huey'])

    with assert_query_count(1):
        await assert_users(n5.users, ['zaizee'])

    n6 = await Note.create(text='note-6')
    await assert_users(n6.users, [])


async def test_desciptor_filtering(tables):
    await create_user_notes()
    await create_relationship()

    charlie, huey, mickey, zaizee = await User.select().order_by(User.username)

    with assert_query_count(1):
        notes = charlie.notes.order_by(Note.text.desc())
        await assert_notes(notes, [2, 1])

    with assert_query_count(1):
        notes = huey.notes.where(Note.text != 'note-3')
        await assert_notes(notes, [2])


async def test_set_values(tables):
    await create_user_notes()

    charlie = await User.get(User.username == 'charlie')
    huey = await User.get(User.username == 'huey')
    n1, n2, n3, n4, n5 = await Note.select().order_by(Note.text)

    with assert_query_count(2):
        await charlie.notes.set(n1)
    await assert_notes(charlie.notes, [1])
    await assert_users(n1.users, ['charlie'])

    await charlie.notes.set([n2, n3])
    await assert_notes(charlie.notes, [2, 3])
    await assert_users(n1.users, [])
    await assert_users(n2.users, ['charlie'])
    await assert_users(n3.users, ['charlie'])

    with assert_query_count(2):
        await huey.notes.set(Note.select().where(~(Note.text.endswith('4'))))
    await assert_notes(huey.notes, [1, 2, 3, 5])


async def test_add(tables):
    await create_user_notes()

    charlie = await User.get(User.username == 'charlie')
    huey = await User.get(User.username == 'huey')
    n1, n2, n3, n4, n5 = await Note.select().order_by(Note.text)

    await charlie.notes.add([n1, n2])
    await assert_notes(charlie.notes, [1, 2])
    await assert_users(n1.users, ['charlie'])
    await assert_users(n2.users, ['charlie'])
    others = [n3, n4, n5]
    for note in others:
        await assert_users(note.users, [])

    with assert_query_count(1):
        await huey.notes.add(Note.select().where(
            fn.substr(Note.text, 6, 1) << ['1', '3', '5']))
    await assert_notes(huey.notes, [1, 3, 5])
    await assert_users(n1.users, ['charlie', 'huey'])
    await assert_users(n2.users, ['charlie'])
    await assert_users(n3.users, ['huey'])
    await assert_users(n4.users, [])
    await assert_users(n5.users, ['huey'])

    with assert_query_count(1):
        await charlie.notes.add(n4)
    await assert_notes(charlie.notes, [1, 2, 4])

    with assert_query_count(2):
        await n3.users.add(
            User.select().where(User.username != 'charlie'),
            clear_existing=True)
    await assert_users(n3.users, ['huey', 'mickey', 'zaizee'])


async def test_add_by_ids(tables):
    await create_user_notes()

    charlie = await User.get(User.username == 'charlie')
    n1, n2, n3 = await Note.select().order_by(Note.text).limit(3)
    await charlie.notes.add([n1.id, n2.id])
    await assert_notes(charlie.notes, [1, 2])
    await assert_users(n1.users, ['charlie'])
    await assert_users(n2.users, ['charlie'])
    await assert_users(n3.users, [])


async def test_unique(tables):
    await create_user_notes()

    n1 = await Note.get(Note.text == 'note-1')
    charlie = await User.get(User.username == 'charlie')

    async def add_user(note, user):
        with assert_query_count(1):
            await note.users.add(user)

    await add_user(n1, charlie)
    with pytest.raises(IntegrityError):
        await add_user(n1, charlie)

    await add_user(n1, await User.get(User.username == 'zaizee'))
    await assert_users(n1.users, ['charlie', 'zaizee'])


async def test_remove(tables):
    await create_user_notes()
    await create_relationship()

    charlie, huey, mickey, zaizee = await User.select().order_by(User.username)
    n1, n2, n3, n4, n5 = await Note.select().order_by(Note.text)

    with assert_query_count(1):
        await charlie.notes.remove([n1, n2, n3])

    await assert_notes(charlie.notes, [])
    await assert_notes(huey.notes, [2, 3])

    with assert_query_count(1):
        await huey.notes.remove(Note.select().where(
            Note.text << ['note-2', 'note-4', 'note-5']))

    await assert_notes(huey.notes, [3])
    await assert_notes(mickey.notes, [3, 4])
    await assert_notes(zaizee.notes, [4, 5])

    with assert_query_count(1):
        await n4.users.remove([charlie, mickey])
    await assert_users(n4.users, ['zaizee'])

    with assert_query_count(1):
        await n5.users.remove(User.select())
    await assert_users(n5.users, [])


async def test_remove_by_id(tables):
    await create_user_notes()
    await create_relationship()

    charlie, huey, mickey, zaizee = await User.select().order_by(User.username)
    n1, n2, n3, n4, n5 = await Note.select().order_by(Note.text)
    await charlie.notes.add([n3, n4])

    with assert_query_count(1):
        await charlie.notes.remove([n1.id, n3.id])

    await assert_notes(charlie.notes, [2, 4])
    await assert_notes(huey.notes, [2, 3])


async def test_clear(tables):
    await create_user_notes()

    charlie = await User.get(User.username == 'charlie')
    huey = await User.get(User.username == 'huey')

    await charlie.notes.set(Note.select())
    await huey.notes.set(Note.select())

    assert await charlie.notes.count() == 5
    assert await huey.notes.count() == 5

    await charlie.notes.clear()
    assert await charlie.notes.count() == 0
    assert await huey.notes.count() == 5

    n1 = await Note.get(Note.text == 'note-1')
    n2 = await Note.get(Note.text == 'note-2')

    await n1.users.set(User.select())
    await n2.users.set(User.select())

    assert await n1.users.count() == 4
    assert await n2.users.count() == 4

    await n1.users.clear()
    assert await n1.users.count() == 0
    assert await n2.users.count() == 4


async def test_manual_through(tables):
    await create_user_notes()

    charlie, huey, mickey, zaizee = await User.select().order_by(User.username)
    alt_notes = []
    for i in range(5):
        alt_notes.append(await AltNote.create(text='note-%s' % (i + 1)))

    await assert_notes(charlie.altnotes, [])
    for alt_note in alt_notes:
        await assert_users(alt_note.users, [])

    n1, n2, n3, n4, n5 = alt_notes

    # Test adding relationships by setting the descriptor.
    await charlie.altnotes.set([n1, n2])

    with assert_query_count(2):
        await huey.altnotes.set(AltNote.select().where(
            fn.substr(AltNote.text, 6, 1) << ['1', '3', '5']))

    await mickey.altnotes.add([n1, n4])

    with assert_query_count(2):
        await zaizee.altnotes.set(AltNote.select())

    # Test that the notes were added correctly.
    with assert_query_count(1):
        await assert_notes(charlie.altnotes, [1, 2])

    with assert_query_count(1):
        await assert_notes(huey.altnotes, [1, 3, 5])

    with assert_query_count(1):
        await assert_notes(mickey.altnotes, [1, 4])

    with assert_query_count(1):
        await assert_notes(zaizee.altnotes, [1, 2, 3, 4, 5])

    # Test removing notes.
    with assert_query_count(1):
        await charlie.altnotes.remove(n1)
    await assert_notes(charlie.altnotes, [2])

    with assert_query_count(1):
        await huey.altnotes.remove([n1, n2, n3])
    await assert_notes(huey.altnotes, [5])

    with assert_query_count(1):
        await zaizee.altnotes.remove(
            AltNote.select().where(
                fn.substr(AltNote.text, 6, 1) << ['1', '2', '4']))
    await assert_notes(zaizee.altnotes, [3, 5])

    # Test the backside of the relationship.
    await n1.users.set(User.select().where(User.username != 'charlie'))

    with assert_query_count(1):
        await assert_users(n1.users, ['huey', 'mickey', 'zaizee'])
    with assert_query_count(1):
        await assert_users(n2.users, ['charlie'])
    with assert_query_count(1):
        await assert_users(n3.users, ['zaizee'])
    with assert_query_count(1):
        await assert_users(n4.users, ['mickey'])
    with assert_query_count(1):
        await assert_users(n5.users, ['huey', 'zaizee'])

    with assert_query_count(1):
        await n1.users.remove(User.select())
    with assert_query_count(1):
        await n5.users.remove([charlie, huey])

    with assert_query_count(1):
        await assert_users(n1.users, [])
    with assert_query_count(1):
        await assert_users(n5.users, ['zaizee'])


# class Person(BaseModel):
#     name = CharField()

# class Soul(BaseModel):
#     person = ForeignKeyField(Person, primary_key=True)

# class SoulList(BaseModel):
#     name = CharField()
#     souls = ManyToManyField(Soul, related_name='lists')

# SoulListThrough = SoulList.souls.get_through_model()

# class TestForeignKeyPrimaryKeyManyToMany(ModelTestCase):
#     requires = [Person, Soul, SoulList, SoulListThrough]
#     test_data = (
#         ('huey', ('cats', 'evil')),
#         ('zaizee', ('cats', 'good')),
#         ('mickey', ('dogs', 'good')),
#         ('zombie', ()),
#     )

#     def setUp(self):
#         super(TestForeignKeyPrimaryKeyManyToMany, self).setUp()

#         name2list = {}
#         for name, lists in self.test_data:
#             p = Person.create(name=name)
#             s = Soul.create(person=p)
#             for l in lists:
#                 if l not in name2list:
#                     name2list[l] = SoulList.create(name=l)
#                 name2list[l].souls.add(s)

#     def soul_for(self, name):
#         return Soul.select().join(Person).where(Person.name == name).get()

#     def assertLists(self, l1, l2):
#         assert sorted(list(l1)), sorted(list(l2)))

#     def test_pk_is_fk(self):
#         list2names = {}
#         for name, lists in self.test_data:
#             soul = self.soul_for(name)
#             self.assertLists([l.name for l in soul.lists],
#                              lists)
#             for l in lists:
#                 list2names.setdefault(l, [])
#                 list2names[l].append(name)

#         for list_name, names in list2names.items():
#             soul_list = SoulList.get(SoulList.name == list_name)
#             self.assertLists([s.person.name for s in soul_list.souls],
#                              names)

#     def test_empty(self):
#         sl = SoulList.create(name='empty')
#         assert list(sl.souls), [])
