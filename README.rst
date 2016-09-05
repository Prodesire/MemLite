MemLite
=============

MemLite is

* a fast, pure-Python, untyped, in-memory database engine, using
  Python syntax to manage data, instead of SQL, inspired by PyDbLite.


Supported Python versions: 2.6-2.7

Usage
---------------

Create fields

.. code-block:: bash

    import memLite
    db = memlite.Base()
    db.create('a', 'b', 'c')

Create indexs:

.. code-block:: bash

    db.create_index('a', 'b')

Insert a piece of data:

.. code-block:: bash

    db.insert(a=-1, b=0, c=1)

Query data under condition:

.. code-block:: bash

    db.query(a=-1, b=0)
