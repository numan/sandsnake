Sandsnake
=========
.. image:: https://secure.travis-ci.org/numan/sandsnake.png?branch=master
        :target: https://secure.travis-ci.org/numan/sandsnake

Introduction
~~~~~~~~~~~~
**Sandsnake** is a sorted index implemented in python and uses ``redis`` as a backend.
It was originally implemented to be used as an indexing system for `Sunspear <https://www.github.com/numan/sunspear/>`_.

Requirements
------------

Required
~~~~~~~~

Requirements **should** be handled by setuptools, but if they are not, you will need the following Python packages:

* nydus
* redis
* dateutil

Optional
~~~~~~~~

* hiredis


sandsnake.create_sandsnake_backend
----------------------------------

Creates an sandsnake object that allows to to store and retrieve indexes::

    >>> from sandsnake import create_sandsnake_backend
    >>>
    >>> sandsnake = create_sandsnake_backend({
    >>>     'backend': 'sandsnake.backends.redis.Redis',
    >>>     'settings': {
    >>>         'defaults': {
    >>>             'host': 'localhost',
    >>>             'port': 6379,
    >>>             'db': 0,
    >>>         },
    >>>         'hosts': [{'db': 0}, {'db': 1}, {'host': 'redis.example.org'}]
    >>>     },
    >>> })

Internally, the ``Redis`` sandsnake backend uses ``nydus`` to distribute your indexes data over your cluster of redis instances.

There are two required arguements:

* ``backend``: full path to the backend class, which should extend sandsnake.backends.base.BaseSandsnakeBackend
* ``settings``: settings required to initialize the backend. For the ``Redis`` backend, this is a list of hosts in your redis cluster.


Example Usage
--------------

Redis
~~~~~

::

    from sandsnake import create_sandsnake_backend
    import datetime

    sandsnake = create_sandsnake_backend({
        "backend": "sandsnake.backends.redis.Redis",
        "settings": {
            "hosts": [{"db": 5}]
        },
    })

    #add a value of "abc" to the ``index`` for an ``object``
    sandsnake.add("user:1", "homefeed", "abc")
    sandsnake.add("user:1", ["homefeed", "recogfeed"], "abc")
