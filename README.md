# XActor: A Distributed Actor Programming Framework for Python

XActor implements a simple classical actor framework in Python.

XActor currently doesn't implement classical actor model's exception handling,
where execptions in the child actors are passed to parent actors.

Actors in XActor can communicate with each other by sending messages.
XActor uses the communicating event loop version of the Actor model.
Which means that all actors running in a single process share the inbox/message queue.

Within XActor Framework any class can be an Actor.
However, the class' constructor arguments must be pickleable.
If the class implementing the actor is pickleable
then moving actors from one process to another is allowed.
