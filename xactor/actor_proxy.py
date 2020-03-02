"""Actor Proxy."""

from .message import Message
from .actor_system import send, create_actor

class ActorProxy:
    """A proxy of an actor.

    This class provides syntactic sugar for creating and sending messages
    to remote actors.

    The following code shows how to send messages using actor proxy

    >>> actor = ActorProxy(rank, actor_id)
    >>> actor.method(*args, **kwargs)

    The above does the same thing as the following code:

    >>> message = Message("method", args, kwargs)
    >>> send(rank, actor_id, message)

    NOTE: When constructing messages using actor proxy,
    the keyword argument ``buffer_`` is handled specially.
    If present and true,
    it is taken to indicate that the `send` should be called
    with ``immediate=False``.

    Attributes
    ----------
    rank_: int or list of ints
        Rank of the remote actor (see `send` for details)
    actor_id_: str
        ID of the remote actor
    """

    def __init__(self, rank=None, actor_id=None):
        """Initialize.

        Parameters
        ----------
        rank: int or list of ints
            Rank of the remote actor (see `send` for details)
        actor_id: str
            ID of the remote actor
        """
        self.rank_ = rank
        self.actor_id_ = actor_id

        self._method = None

    def __getattr__(self, method):
        """Prepare the proxy for a remote message send.

        Parameters
        ----------
        method: str
            Message method name
        """
        if method.startswith("__"):
            raise AttributeError(
                "Calling dunder methods using ActorProxy is not allowed."
            )

        self._method = method
        return self

    def __call__(self, *args, **kwargs):
        """Setup the args and kwargs for the message and send it.

        NOTE: The keyword argument `buffer_` is handled specially.
        If present and true,
        it is taken to indicate that the `send` should be called
        with ``immediate=False``.

        Parameters
        ----------
        *args: list
            Positional arguments of the Message
        **kwargs: dict
            Keyword arguments of the Message
        """
        if self._method is None:
            raise ValueError("Message method not set")

        immediate = not kwargs.pop("buffer_", False)
        message = Message(self._method, args, kwargs)
        send(self.rank_, self.actor_id_, message, immediate)

        self._method = None

    def __getstate__(self):
        """Return pickleable state."""
        return (self.rank_, self.actor_id_)

    def __setstate__(self, state):
        """Set the state."""
        self.rank_, self.actor_id_ = state

    def create_actor_(self, cls, *args, **kwargs):
        """Create the remote actor.

        Parameters
        ----------
        cls: type
            Class used to instantiate the new actor
        *args: list
            Positional arguments for the constructor
        **kwargs: dict
            Keyword arguments for the constructor
        """
        create_actor(self.rank_, self.actor_id_, cls, *args, **kwargs)
