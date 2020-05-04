XActor API
==========

Staring and Stopping the Actor System
-------------------------------------

.. autofunction:: xactor.actor_system.start
.. autofunction:: xactor.actor_system.stop


Creating and Deleting Actors
----------------------------

.. autofunction:: xactor.actor_system.create_actor
.. autofunction:: xactor.actor_system.delete_actors

Sending Messages
----------------

.. autofunction:: xactor.actor_system.send
.. autofunction:: xactor.actor_system.flush

Directly Accessing Local Actors
-------------------------------

.. autofunction:: xactor.actor_system.local_actor

Nodes and Ranks
---------------

.. autofunction:: xactor.actor_system.ranks
.. autofunction:: xactor.actor_system.nodes
.. autofunction:: xactor.actor_system.node_ranks
.. autofunction:: xactor.actor_system.current_rank

.. autodata:: xactor.actor_system.MASTER_RANK
    :annotation:

.. autodata:: xactor.actor_system.EVERY_RANK
    :annotation:

Message
-------

.. autoclass:: xactor.message.Message

ActorProxy
----------

.. autoclass:: xactor.actor_proxy.ActorProxy
    :members:

Utilities
---------

.. autofunction:: xactor.actor_system.getLogger

Environment Variables
---------------------

.. automodule:: xactor.evars

