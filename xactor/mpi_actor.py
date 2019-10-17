"""A simple Actor API built on top of MPI.

Provides a rudimentary classical Actor model implementation on top of MPI.
"""

__all__ = [
    "Message",
    "get_nodes",
    "get_node_ranks",
    "start",
    "stop",
    "send",
    "flush",
    "barrier",
    "WORLD_SIZE",
    "WORLD_RANK",
    "MASTER_RANK",
    "RANK_AID_FMT",
    "MAIN_AID",
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

RANK_AID_FMT = "rank-%d"
MAIN_AID = "main"

log = logging.getLogger("%s.%d" % (__name__, WORLD_RANK))


@dataclass(init=False)
class Message:
    """A Message."""

    actor_id: str
    method: str
    args: list
    kwargs: dict

    def __init__(self, actor_id, method, *args, **kwargs):
        """Construct the message."""
        self.actor_id = actor_id
        self.method = method
        self.args = args
        self.kwargs = kwargs


class NodeRanks:
    """Get the rank of the jth process on the ith node."""

    def __init__(self):
        """Initialize."""
        rank_nodes = COMM_WORLD.allgather(HOSTNAME)

        node_ranks = defaultdict(list)
        for rank, hostname in enumerate(rank_nodes):
            node_ranks[hostname].append(rank)
        nodes = sorted(node_ranks)

        self.nodes = nodes
        self.node_ranks = dict(node_ranks)

    def get_nodes(self):
        """Return the nodes running xactor.

        Returns
        -------
            nodes: list of node names
        """
        return self.nodes

    def get_node_ranks(self, node):
        """Return the ranks on the currnet node.

        Parameters
        ----------
            node: a node name

        Returns
        -------
            ranks: list of ranks running on the given node.
        """
        return self.node_ranks[node]


_NODE_RANKS = NodeRanks()
get_nodes = _NODE_RANKS.get_nodes
get_node_ranks = _NODE_RANKS.get_node_ranks


class MPIProcess:
    """MPI Process.

    Container for actors that runs on the current rank.
    """

    def __init__(self):
        self.acomm = AsyncCommunicator()
        self.local_actors = {RANK_AID_FMT % WORLD_RANK: self}

        self.stopping = False

    def _loop(self):
        """Loop through messages."""
        log.info("Starting rank loop with %d actors", len(self.local_actors))

        while not self.stopping:
            message = self.acomm.recv()
            if message.actor_id not in self.local_actors:
                raise RuntimeError("Message received for non-local actor: %r" % message)

            actor = self.local_actors[message.actor_id]
            try:
                method = getattr(actor, message.method)
            except AttributeError:
                log.exception(
                    "Target actor doesn't have requested method: %r, %r", actor, message
                )
                raise

            try:
                method(*message.args, **message.kwargs)
            except Exception:  # pylint: disable=broad-except
                log.exception(
                    "Exception occured while processing message: %r, %r", actor, message
                )
                raise

    def _stop(self):
        """Stop the event loop after processing the current message."""
        log.info("Received stop message")

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
            try:
                del self.local_actors[actor_id]
            except KeyError:
                raise RuntimeError("Actor with ID %s doesn't exist" % actor_id)

    def send_actors(self, actor_ids, dst_ranks):
        """Send the local actors to MPI Processes on the destination ranks.

        Parameters
        ----------
            actor_ids: IDs of local actors to be moved out
            dst_ranks: Ranks to which the actors are to be sent
        """
        if not len(actor_ids) == len(dst_ranks):
            raise ValueError(
                "len(actor_ids) = (%d) != len(dst_ranks) (%d)"
                % (len(actor_ids), len(dst_ranks))
            )

        for actor_id, dst_rank in zip(actor_ids, dst_ranks):
            if actor_id not in self.local_actors:
                raise RuntimeError("Actor with ID %s doesn't exist" % actor_id)

            dst_id = RANK_AID_FMT % dst_rank
            actor = self.local_actors[actor_id]
            msg = Message(dst_id, "receive_actor", actor_id, actor)
            self.acomm.send(dst_rank, msg)

        self.acomm.flush()

    def receive_actor(self, actor_id, actor):
        """Receive an actor another MPI Process."""
        if actor_id in self.local_actors:
            raise RuntimeError("Actor with ID %s already exists" % actor_id)

        self.local_actors[actor_id] = actor

    def start(self, cls, *args, **kwargs):
        """Start the MPI process.

        Parameters
        ----------
            cls: The main class, instantiated and its `main' method executed on MASTER_RANK
            *arg: Positional arguments for the class
            **kwargs: Keyword arguments for the class
        """
        if WORLD_RANK == MASTER_RANK:
            self.create_actor(MAIN_AID, cls, args, kwargs)

            msg = Message(MAIN_AID, "main")
            self.acomm.send(MASTER_RANK, msg)
            self.acomm.flush()

        self._loop()

    def stop(self):
        """Stop all MPI Processes."""
        for rank in range(WORLD_SIZE):
            dst_id = RANK_AID_FMT % rank
            msg = Message(dst_id, "_stop")
            self.acomm.send(rank, msg)

        self.acomm.flush()

    def send(self, rank, message, flush=True):  # pylint: disable=redefined-outer-name
        """Send a message to the given rank.

        Parameters
        ----------
            rank: Destination rank
            message: Message to be sent
            flush: If true ensure that any send buffers are flushed
        """
        self.acomm.send(rank, message)
        if flush:
            self.acomm.flush()

    def flush(self):
        """Flush any send buffers."""
        self.acomm.flush()

    @staticmethod
    def barrier():
        """Perform a barrier synchornization."""
        COMM_WORLD.Barrier()


_MPI_PROCESS = MPIProcess()
start = _MPI_PROCESS.start
stop = _MPI_PROCESS.stop
send = _MPI_PROCESS.send
flush = _MPI_PROCESS.flush
barrier = _MPI_PROCESS.barrier
