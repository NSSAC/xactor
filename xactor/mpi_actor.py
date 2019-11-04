"""A simple Actor API built on top of MPI.

Provides a rudimentary classical Actor model implementation on top of MPI.
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
    "MASTER_RANK",
    "RANK_ACTOR_ID",
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

RANK_ACTOR_ID = "_rank_actor"

LOG = logging.getLogger("%s.%d" % (__name__, WORLD_RANK))


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

    def get_nodes(self):
        """Return the nodes running xactor.

        Returns
        -------
            nodes: list of node names
        """
        return self.nodes_

    def get_node_ranks(self, node):
        """Return the ranks on the currnet node.

        Parameters
        ----------
            node: a node name

        Returns
        -------
            ranks: list of ranks running on the given node.
        """
        return self.node_ranks_[node]


_NODE_RANKS = NodeRanks()
nodes = _NODE_RANKS.get_nodes
node_ranks = _NODE_RANKS.get_node_ranks


def ranks():
    """Return an iterable of all ranks running xactor.

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
                raise RuntimeError("Message received for non-local actor: %r" % message)

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
        LOG.info("Received stop message.")

        self.acomm.finish()
        self.stopping = True

    def create_actor(self, actor_id, cls, args=None, kwargs=None):
        """Create a local actor.

        Parameters
        ----------
            actor_id: identifier for the new actor
            cls: Class used to instantiate the new actor
            args: Positional arguments for the constructor
            kwargs: Keyword arguments for the constructor
        """
        if actor_id in self.local_actors:
            raise RuntimeError("Actor with ID %s already exists" % actor_id)

        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}

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

    def start(self, actor_id, cls, *args, **kwargs):
        """Start the MPI process.

        Parameters
        ----------
            actor_id: ID of the main actor.
            cls: The main class, instantiated and its `main' method executed on MASTER_RANK
            *arg: Positional arguments for the class
            **kwargs: Keyword arguments for the class
        """
        if WORLD_RANK == MASTER_RANK:
            if actor_id in self.local_actors:
                raise RuntimeError("Actor with ID %s already exists" % actor_id)

            self.create_actor(actor_id, cls, args, kwargs)

            msg = Message("main")
            self.acomm.send(MASTER_RANK, (actor_id, msg))
            self.acomm.flush()

        self._loop()

    def stop(self):
        """Stop all MPI Processes."""
        for rank in range(WORLD_SIZE):
            msg = Message("_stop")
            self.acomm.send(rank, (RANK_ACTOR_ID,  msg))

        self.acomm.flush()

    def send(self, rank, actor_id, message, everynode=False, immediate=True): #pylint: disable=redefined-outer-name
        """Send a message to the given rank.

        Parameters
        ----------
            actor_id: Actor to whom the message is to be sent
            message: Message to be sent
            rank: Destination rank; if None (default) the message is sent to all nodes
            everynode: If rank is not none and everynode is true, message is sent to that rank-th process on every node
            immediate: If true (default) all send buffers are flushed immediately
        """
        if rank == -1:
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
            self.acomm.send(rank_, (actor_id, message))

        if immediate:
            self.acomm.flush()

    def flush(self):
        """Flush any send buffers."""
        self.acomm.flush()


_MPI_RANK_ACTOR = MPIRankActor()
start = _MPI_RANK_ACTOR.start
stop = _MPI_RANK_ACTOR.stop
send = _MPI_RANK_ACTOR.send
flush = _MPI_RANK_ACTOR.flush
