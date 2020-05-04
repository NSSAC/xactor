XActor: A Distributed Actor Programming Framework
=================================================

XActor is a framework for doing distributed computing in Python,
using the actor model of programming.

XActor's documentation is hosted at https://xactor.readthedocs.io/en/latest

Compiling latest documentation
------------------------------

To compile the latest version of the documentation from source,
please check ``docs/source/index.rst``.

XActor depends on mpi4py which requires a MPI implementation
and compiler tools be installed on the system.

Installing MPICH and mpi4py inside a conda environment
.........................................................

To create a new virtual environment with conda,
have Anaconda/Miniconda setup on your system.
Installation instructions for Anaconda can be found
`here <https://docs.conda.io/projects/conda/en/latest/user-guide/install/>`_.
After installation of Anaconda/Miniconda
execute the following commands:

.. code:: bash

    $ conda create -n xactor -c conda-forge python=3 mpich mpi4py

The above command creates a new conda environment called ``xactor``
with python, mpich and mpi4py installed.

The following commands assume you are inside the above conda environment.

Building the documentation from source
......................................

To build and view the documentation as HTML, execute the following commands:

.. code:: bash

    $ git clone https://github.com/NSSAC/xactor.git
    $ cd xactor
    $ pip install -e .
    $ pip install -r docs/requirements.txt
    $ make -C docs
    $ <BROWSER-COMMAND> docs/build/html/index.html
