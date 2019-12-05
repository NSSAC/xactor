"""A Message."""

from dataclasses import dataclass, field

@dataclass
class Message:
    """A Message.

    A message corresponds to a method call on the remote actor.
    A message objects contains the name of the method,
    and the parameters to be passed when calling.

    A message can be constructed directly,
    or by using a `ActorProxy` object.

    Attributes
    ----------
    method: str
        Name of the method
    args: list
        Positional arguments
    kwargs: dict
        Keyword arguments
    """

    method: str
    args: list = field(default_factory=list)
    kwargs: dict = field(default_factory=dict)
