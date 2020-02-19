"""A simple actor system API built on top of MPI."""

import logging
from collections import defaultdict

from mpi4py import MPI

from .message import Message
from .mpi_rank_actor import MPIRankActor, RANK_ACTOR_ID

COMM_WORLD = MPI.COMM_WORLD
HOSTNAME = MPI.Get_processor_name()
WORLD_RANK = COMM_WORLD.Get_rank()
WORLD_SIZE = COMM_WORLD.Get_size()

MASTER_RANK = 0
"""Constant used to refer to the designated master rank."""

EVERY_RANK = -1
"""Constant used to send a message to all ranks."""

_MPI_RANK_ACTOR = None
_NODES = None
_NODE_RANKS = None

LOG = logging.getLogger("%s.%d" % (__name__, WORLD_RANK))


def getLogger(name):
    """Return a logger with the given name and the world rank appended to it.

    Parameters
    ----------
    name: str
        Name of the logger

    Returns
    -------
    logger: logging.Logger
        A logging.Logger object
    """
    name = "%s.%d" % (name, current_rank())
    return logging.getLogger(name)


def get_node_ranks():
    """Get nodes where we are running and the ranks on the nodes."""
    rank_nodes = COMM_WORLD.allgather(HOSTNAME)

    node_ranks_ = defaultdict(list)
    for rank, hostname in enumerate(rank_nodes):
        node_ranks_[hostname].append(rank)
    nodes_ = sorted(node_ranks_)

    return nodes_, node_ranks_


def send(rank, actor_id, message, immediate=False):
    """Send the message to the given actor on the given rank.

    Parameters
    ----------
    rank: int or list of ints
        Destination rank(s) on which the actor resides.
        If rank is an iterable, send it to all ranks in the iterable.
        if rank == EVERY_RANK, message is sent to all ranks.
    actor_id: str
        Actor to whom the message is to be sent
    message: Message
        Message to be sent
    immediate: bool
        If true flush out all send buffers
    """
    if isinstance(rank, int):
        if rank == EVERY_RANK:
            ranks_ = range(WORLD_SIZE)
        else:
            ranks_ = [rank]
    else:
        ranks_ = list(rank)

    for rank_ in ranks_:
        _MPI_RANK_ACTOR.send(rank_, actor_id, message)

    if immediate:
        _MPI_RANK_ACTOR.flush()


def create_actor(rank, actor_id, cls, *args, **kwargs):
    """Create an actor on the given rank.

    Parameters
    ----------
    rank: int or list of ints
        Rank(s) on which actor is to be created.
        See `send` for how `rank` is processed.
    actor_id: str
        ID of the new actor
    cls: type
        Class used to instantiate the new actor
    *args: list
        Positional arguments for the constructor
    **kwargs: dict
        Keyword arguments for the constructor
    """
    message = Message("create_actor", args=[actor_id, cls, args, kwargs])
    send(rank, RANK_ACTOR_ID, message, immediate=True)


def delete_actors(rank, actor_ids):
    """Delete the given actors on the given rank.

    Parameters
    ----------
    rank: int or list of ints
        Rank(s) on which actor is to be created.
        See `send` for how `rank` is processed.
    actor_ids: list of str
        IDs of actors to be deleted
    """
    message = Message("delete_actors", args=[actor_ids])
    send(rank, RANK_ACTOR_ID, message, immediate=True)

def start(actor_id, cls, *args, **kwargs):
    """Start the actor system.

    This method starts up the rank actors,
    creates the main actor on the MASTER_RANK (using given arguments),
    and sends it the "main" message.

    Parameters
    ----------
    actor_id: str
        ID of the main actor.
    cls: type
        Class used to instantiate the `main` actor on the MASTER_RANK
    *args: list
        Positional arguments for the constructor
    **kwargs: dict
        Keyword arguments for the constructor
    """
    global _NODES, _NODE_RANKS, _MPI_RANK_ACTOR  # pylint: disable=global-statement

    try:
        if _MPI_RANK_ACTOR is not None:
            raise ValueError("The actor system has already been started.")

        COMM_WORLD.Barrier()

        _NODES, _NODE_RANKS = get_node_ranks()
        _MPI_RANK_ACTOR = MPIRankActor()

        if WORLD_RANK == MASTER_RANK:
            # Create main actor
            if __debug__:
                LOG.debug("Creating main actor: id=%s class=%r", actor_id, cls)
            _MPI_RANK_ACTOR.create_actor(actor_id, cls, args, kwargs)

            # Schedule message to be delivered to main actor
            message = Message("main")
            _MPI_RANK_ACTOR.send(MASTER_RANK, actor_id, message)
            _MPI_RANK_ACTOR.flush()

        _MPI_RANK_ACTOR._loop()  # pylint: disable=protected-access
    except:  # pylint: disable=bare-except
        LOG.exception("Uncaught exception")
        COMM_WORLD.Abort(1)


def stop():
    """Stop the actor system."""
    message = Message("_stop")
    send(EVERY_RANK, RANK_ACTOR_ID, message, immediate=True)


def flush():
    """Flush out the send buffers."""
    _MPI_RANK_ACTOR.flush()


def ranks():
    """Return all ranks running the actor system.

    Returns
    -------
    ranks: iterable of int
        An iterable of node ranks
    """
    return range(WORLD_SIZE)


def current_rank():
    """Return the rank of current process.

    Returns
    -------
    rank: int
        Rank of the current process
    """
    return WORLD_RANK


def nodes():
    """Return all nodes running the actor system.

    Returns
    -------
    nodes: list
        List of of node names
    """
    return _NODES


def node_ranks(node):
    """Return the ranks on the given node.

    Parameters
    ----------
    node: str
        The name of a node name

    Returns
    -------
    ranks: list of int
        List of ranks running on the given node.
    """
    return _NODE_RANKS[node]

def local_actor(self, actor_id):
    """Return the reference to the local actor.

    Parameters
    ----------
    actor_id: str
        ID of the already existing actor.

    Returns
    -------
        A reference to the local actor or None if the actor doesn't exist.
    """
    return _MPI_RANK_ACTOR.local_actors.get(actor_id, None)
