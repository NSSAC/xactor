XActor: A Distributed Actor Programming Framework
=================================================

XActor is a framework for doing distributed computing in Python,
using the actor model of programming.

XActor provides a simple classical actor framework,
that is actors communicate with each other by sending messages.
Current version of XActor doesn’t provide futures.
XActor currently doesn’t implement exception chaining,
where execptions in the child actors are passed to parent actors.
XActor uses the communicating event loop version of the Actor model,
which means that all actors running in a single process
share the inbox/message queue.
