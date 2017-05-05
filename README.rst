|Build Status|


AioPeewee
=========

Asyncio interface for peewee_ modeled after torpeewee_

Limitations (hopefully resolved in a couple of weeks):

- currently just aiomysql supported
- untested transactions

More thorough testing is on the way.

Any feedback would be greatly appreciated!


Install
-------

.. code:: bash

    pip install aiopeewee


Usage
-----

.. code:: python

    from aiopeewee import AioModel, AioMySQLDatabase
    from peewee import CharField, TextField, DateTimeField
    from peewee import ForeignKeyField, PrimaryKeyField


    db = AioMySQLDatabase('test', host='127.0.0.1', port=3306,
                         user='root', password='')


    class User(AioModel):
        username = CharField()

        class Meta:
            database = db


    class Blog(AioModel):
        user = ForeignKeyField(User)
        title = CharField(max_length=25)
        content = TextField(default='')
        pub_date = DateTimeField(null=True)
        pk = PrimaryKeyField()

        class Meta:
            database = db
 
   
    # create connection pool
    await db.connect(loop)

    # count
    await User.select().count()

    # async iteration on select query
    async for user in User.select():
        print(user)

    # fetch all records as a list from a query in one pass
    users = await User.select()

    # insert
    user = await User.create(username='kszucs')

    # modify
    user.username = 'krisztian'
    await user.save()

    # async iteration on blog set
    [b.title async for b in user.blog_set.order_by(Blog.title)]

    # close connection pool
    await db.close()

    # see more in the tests


ManyToMany
----------

Note that `AioManyToManyField` must be used instead of `ManyToMany`.


.. code:: python

    from aiopeewee import AioManyToManyField


    class User(AioModel):
        username = CharField(unique=True)

        class Meta:
            database = db


    class Note(AioModel):
        text = TextField()
        users = AioManyToManyField(User)

        class Meta:
            database = db


    NoteUserThrough = Note.users.get_through_model()


    async for user in note.users:
        # do something with the users


Currently the only limitation I'm aware of immidiate setting of instance relation must be replaced with a method call:

.. code:: python

    # original, which is not supported
    charlie.notes = [n2, n3]

    # use instead
    await charlie.notes.set([n2, n3])


Serializing
-----------

Converting to dict requires the asyncified version of `model_to_dict` 

.. code:: python

    from aiopeewee import model_to_dict

    serialized = await model_to_dict(user)

         
.. _peewee: http://docs.peewee-orm.com/en/latest/
.. _torpeewee: https://github.com/snower/torpeewee

.. |Build Status| image:: http://drone.lensa.com:8000/api/badges/kszucs/aiopeewee/status.svg
   :target: http://drone.lensa.com:8000/kszucs/pandahouse


