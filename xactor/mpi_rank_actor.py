"""MPI Rank Actor."""

import traceback
import logging
import cProfile as profile


from .mpi_acomm import AsyncCommunicator, WORLD_RANK
from .evars import get_profile_dir, do_send_traceback

RANK_ACTOR_ID = "_rank_actor"
LOG = logging.getLogger("%s.%d" % (__name__, WORLD_RANK))


class MPIRankActor:
    """MPI Rank Actor.

    Container for actors that run on the current rank.
    """

    def __init__(self):
        self.acomm = AsyncCommunicator()
        self.local_actors = {RANK_ACTOR_ID: self}

        self.stopping = False

        self.profile = None
        self.send_traceback = False
        if __debug__:
            if get_profile_dir() is not None:
                self.profile = profile.Profile()
                self.profile.disable()
            self.send_traceback = do_send_traceback()

    def _loop(self):
        """Loop through messages."""
        LOG.debug("Starting rank loop with %d actors", len(self.local_actors))

        while not self.stopping:
            if __debug__:
                actor_id, sender_st, message = self.acomm.recv()
            else:
                actor_id, message = self.acomm.recv()
                sender_st = None

            if actor_id not in self.local_actors:
                LOG.error("Message received for non-local actor: %r", actor_id)
                if sender_st is not None:
                    LOG.error("Sender stack trace\n%s", sender_st)
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
                if sender_st is not None:
                    LOG.error("Sender stack trace\n%s", sender_st)
                raise

            try:
                if __debug__ and self.profile is not None:
                    self.profile.runcall(method, *message.args, **message.kwargs)
                else:
                    method(*message.args, **message.kwargs)
            except Exception:  # pylint: disable=broad-except
                LOG.exception(
                    "Exception occured while processing message: %r, %r", actor, message
                )
                if sender_st is not None:
                    LOG.error("Sender stack trace\n%s", sender_st)
                raise

        if __debug__ and self.profile is not None:
            self.profile.dump_stats("%s/%d.prof" % (get_profile_dir(), WORLD_RANK))

    def _stop(self):
        """Stop the event loop after processing the current message."""
        LOG.debug("Received stop message")

        self.acomm.finish()
        self.stopping = True

    def create_actor(self, actor_id, cls, args, kwargs):
        """Create a local actor.

        Parameters
        ----------
        actor_id: str
            ID of the new actor
        cls: type
            Class used to instantiate the new actor
        args: list
            Positional arguments for the constructor
        kwargs: dict
            Keyword arguments for the constructor
        """
        if actor_id in self.local_actors:
            raise RuntimeError("Actor with ID %s already exists" % actor_id)

        actor = cls(*args, **kwargs)
        self.local_actors[actor_id] = actor

    def delete_actors(self, actor_ids):
        """Delete local actors.

        Parameters
        ----------
        actor_ids: list
            IDs of local actors to be deleted
        """
        for actor_id in actor_ids:
            if actor_id == RANK_ACTOR_ID:
                raise RuntimeError("Can't delete the rank actor.")
            try:
                del self.local_actors[actor_id]
            except KeyError:
                raise RuntimeError("Actor with ID %s doesn't exist" % actor_id)

    def send(self, rank, actor_id, message):
        """Send the message to the given actor on the given rank.

        Parameters
        ----------
        rank: int
            Destination rank on which the actor resides
        actor_id: str
            Actor to whom the message is to be sent
        message: Message
            Message to be sent
        """
        if __debug__:
            if self.send_traceback:
                sender_st = traceback.format_stack()
                sender_st = "".join(sender_st)
                sender_st = "Sender rank: %d\n%s" % (WORLD_RANK, sender_st)
            else:
                sender_st = None

            self.acomm.send(rank, (actor_id, sender_st, message))
        else:
            self.acomm.send(rank, (actor_id, message))

    def flush(self):
        """Flush out the send buffers."""
        self.acomm.flush()
