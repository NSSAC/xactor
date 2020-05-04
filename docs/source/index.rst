XActor: A Distributed Actor Programming Framework
=================================================

Introduction
------------

XActor is a framework for doing distributed computing in Python,
using the actor model of programming.

In XActor any Python object can be an actor.
Actors communicate with each other by sending messages.
Messages in XActor are implemented as method invocations
on the recipient object.
The return value of the above method invocations (if any)
are ignored by the XActor framework.
If the recipient needs to send a response back to the sender,
it needs to send new message to the sender.

XActor provides a simple classical actor framework,
that is actors communicate with each other by sending messages.
Current version of XActor doesn't provide futures.
XActor currently doesn't implement exception chaining,
where exceptions in the child actors are passed to parent actors.
XActor uses the communicating event loop version of the Actor model,
which means that all actors running in a single process
share the inbox/message queue.

Current version of XActor is built on top of MPI (using mpi4py).
Future versions may support other backends.

Installation
------------

XActor depends on mpi4py which requires a MPI implementation
and compiler tools be installed on the system.

Installing MPICH and mpi4py inside a conda environment
.........................................................

To create a new virtual environment with conda,
have Anaconda/Miniconda setup on your system.
Installation instructions for Anaconda can be found
`here <https://docs.conda.io/projects/conda/en/latest/user-guide/install/>`_.
After installation of Anaconda/Miniconda
execute the following commands::

    $ conda create -n xactor -c conda-forge python=3 mpich mpi4py

The above command creates a new conda environment called ``xactor``
with python, mpich and mpi4py installed.

The following commands assume you are inside the above conda environment.

Installing XActor
.................

One can use pip to install XActor for PyPI as follows::

    $ pip install xactor

Dependencies of XActor (including mpi4py) will be installed
as part of the above pip command.

To install xactor from source::

    $ git clone https://github.com/NSSAC/xactor.git
    $ cd xactor
    $ pip install -e .

Hello World
-----------

The following code shows a simple "hello world" example
using XActor.

.. literalinclude:: ../../tests/scripts/hello.py

To execute the above program save it as ``hello.py``.

The following command
executes the above program using two processes
on the current machine::

    $ mpiexec -n 2 python hello.py

API
---

.. toctree::
   :maxdepth: 2

   api
   internal


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
