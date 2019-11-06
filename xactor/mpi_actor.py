"""A simple Actor API built on top of MPI.

Provides a classical actor model implementation on top of MPI.
"""

__all__ = [
    "Message",
    "nodes",
    "ranks",
    "node_ranks",
    "current_rank",
    "start",
    "stop",
    "send",
    "flush",
    "create_actor",
    "delete_actors",
    "MASTER_RANK",
    "EVERY_RANK",
    "getLogger",
]

import logging
from dataclasses import dataclass
from collections import defaultdict

from mpi4py import MPI

from .mpi_acomm import AsyncCommunicator

COMM_WORLD = MPI.COMM_WORLD
HOSTNAME = MPI.Get_processor_name()
WORLD_RANK = COMM_WORLD.Get_rank()
WORLD_SIZE = COMM_WORLD.Get_size()

MASTER_RANK = 0
EVERY_RANK = -1

RANK_ACTOR_ID = "_rank_actor"

_MPI_RANK_ACTOR = None
_NODE_RANKS = None

def getLogger(name):
    """Return a logger with the given name and the world rank attached to it."""
    name = "%s.%d" % (name, WORLD_RANK)
    return logging.getLogger(name)

LOG = getLogger(__name__)

@dataclass(init=False)
class Message:
    """A Message."""

    method: str
    args: list
    kwargs: dict

    def __init__(self, method, *args, **kwargs):
        """Construct the message."""
        self.method = method
        self.args = args
        self.kwargs = kwargs


class NodeRanks:
    """Get the rank of the jth process on the ith node."""

    def __init__(self):
        """Initialize."""
        rank_nodes = COMM_WORLD.allgather(HOSTNAME)

        node_ranks_ = defaultdict(list)
        for rank, hostname in enumerate(rank_nodes):
            node_ranks_[hostname].append(rank)
        nodes_ = sorted(node_ranks_)

        self.nodes_ = nodes_
        self.node_ranks_ = dict(node_ranks_)


class MPIRankActor:
    """MPI Rank Actor.

    Container for actors that runs on the current rank.
    """

    def __init__(self):
        self.acomm = AsyncCommunicator()
        self.local_actors = {RANK_ACTOR_ID: self}

        self.stopping = False

    def _loop(self):
        """Loop through messages."""
        LOG.info("Starting rank loop with %d actors", len(self.local_actors))

        while not self.stopping:
            actor_id, message = self.acomm.recv()
            if actor_id not in self.local_actors:
                raise RuntimeError(
                    "Message received for non-local actor: %r" % actor_id
                )

            actor = self.local_actors[actor_id]
            try:
                method = getattr(actor, message.method)
            except AttributeError:
                LOG.exception(
                    "Target actor doesn't have requested method: %r, %r", actor, message
                )
                raise

            try:
                method(*message.args, **message.kwargs)
            except Exception:  # pylint: disable=broad-except
                LOG.exception(
                    "Exception occured while processing message: %r, %r", actor, message
                )
                raise

    def _stop(self):
        """Stop the event loop after processing the current message."""
        LOG.info("Received stop message")

        self.acomm.finish()
        self.stopping = True

    def create_actor(self, actor_id, cls, args, kwargs):
        """Create a local actor.

        Parameters
        ----------
            actor_id: ID of the new actor
            cls: Class used to instantiate the new actor
            args: Positional arguments for the constructor
            kwargs: Keyword arguments for the constructor
        """
        if actor_id in self.local_actors:
            raise RuntimeError("Actor with ID %s already exists" % actor_id)

        actor = cls(*args, **kwargs)
        self.local_actors[actor_id] = actor

    def delete_actors(self, actor_ids):
        """Delete local actors.

        Parameters
        ----------
            actor_ids: IDs of local actors to be deleted
        """
        for actor_id in actor_ids:
            if actor_id == RANK_ACTOR_ID:
                raise RuntimeError("Can't delete the rank actor.")
            try:
                del self.local_actors[actor_id]
            except KeyError:
                raise RuntimeError("Actor with ID %s doesn't exist" % actor_id)

    def send(self, rank, actor_id, message):  # pylint: disable=redefined-outer-name
        """Send the message to the given actor on the given rank.

        Parameters
        ----------
            rank: Destination rank on which the actor resides
            actor_id: Actor to whom the message is to be sent
            message: Message to be sent
        """
        self.acomm.send(rank, (actor_id, message))

    def flush(self):
        """Flush out the send buffers."""
        self.acomm.flush()


def send(rank, actor_id, message, everynode=False, immediate=True):
    """Send the message to the given actor on the given rank.

    Parameters
    ----------
        rank: Destination rank on which the actor resides
              if rank == EVERY_RANK, message is sent to all ranks
        actor_id: Actor to whom the message is to be sent
        message: Message to be sent
        everynode: If true, message is sent to the rank-th process on every node
        immediate: If true, all send buffers are flushed immediately
    """
    if rank == EVERY_RANK:
        ranks_ = range(WORLD_SIZE)
    else:
        if everynode:
            ranks_ = []
            for n in nodes():
                nrs = node_ranks(n)
                r = nrs[rank % len(nrs)]
                ranks_.append(r)
        else:
            ranks_ = [rank]

    for rank_ in ranks_:
        _MPI_RANK_ACTOR.send(rank_, actor_id, message)

    if immediate:
        _MPI_RANK_ACTOR.flush()


def create_actor(rank, actor_id, cls, *args, **kwargs):
    """Create an actor on the given rank.

    Parameters
    ----------
        rank: Rank on which actor is to be created.
        actor_id: ID of the new actor
        cls: Class used to instantiate the new actor
        *args: Positional arguments for the constructor
        **kwargs: Keyword arguments for the constructor
    """
    message = Message("create_actor", actor_id, cls, args, kwargs)
    send(rank, RANK_ACTOR_ID, message, immediate=True)


def delete_actors(rank, actor_ids):
    """Delete the given actors on the given rank.

    Parameters
    ----------
        rank: Rank on which actors are to be deleted
        actor_ids: IDs of actors to be deleted
    """
    message = Message("delete_actors", actor_ids)
    send(rank, RANK_ACTOR_ID, message, immediate=True)


def start(actor_id, cls, *args, **kwargs):
    """Start the actor system.

    This method starts up the rank actors,
    creates the main actor on the MASTER_RANK (using given arguments),
    and sends it the "main" message.

    Parameters
    ----------
        actor_id: ID of the main actor.
        cls: Class used to instantiate the `main' actor on the MASTER_RANK
        *arg: Positional arguments for the constructor
        **kwargs: Keyword arguments for the constructor
    """
    global _NODE_RANKS, _MPI_RANK_ACTOR  # pylint: disable=global-statement

    if _MPI_RANK_ACTOR is not None:
        raise ValueError("The actor system has already been started.")

    _NODE_RANKS = NodeRanks()
    _MPI_RANK_ACTOR = MPIRankActor()

    if WORLD_RANK == MASTER_RANK:
        _MPI_RANK_ACTOR.create_actor(actor_id, cls, args, kwargs)

        message = Message("main")
        _MPI_RANK_ACTOR.send(MASTER_RANK, actor_id, message)
        _MPI_RANK_ACTOR.flush()

    _MPI_RANK_ACTOR._loop()  # pylint: disable=protected-access


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
        ranks: an iterable of node ranks
    """
    return range(WORLD_SIZE)


def current_rank():
    """Return the rank of current process.

    Returns
    -------
        rank: rank of the current process
    """
    return WORLD_RANK


def nodes():
    """Return all nodes running the actor system.

    Returns
    -------
        nodes: List of of node names
    """
    return _NODE_RANKS.nodes_


def node_ranks(node):
    """Return the ranks on the given node.

    Parameters
    ----------
        node: a node name

    Returns
    -------
        ranks: List of ranks running on the given node.
    """
    return _NODE_RANKS.node_ranks_[node]
