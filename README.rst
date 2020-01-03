XActor: A Distributed Actor Programming Framework
=================================================

XActor is a framework for doing distributed computing in Python,
using the actor model of programming.

Full documentation
------------------

The full documentation is available at ``docs/source/index.rst``

XActor depends on mpi4py which requires a MPI implementation
and compiler tools be installed on the system.

Installing Open MPI and mpi4py inside a conda environment
.........................................................

To create a new virtual environment with conda,
have Anaconda/Miniconda setup on your system.
Installation instructions for Anaconda can be found
`here <https://docs.conda.io/projects/conda/en/latest/user-guide/install/>`_.
After installation of Anaconda/Miniconda
execute the following commands:

.. code:: bash

    $ conda create -n xactor -c conda-forge python=3 openmpi mpi4py

The above command creates a new conda environment called ``xactor``
with python, openmpi and mpi4py installed.

The following commands assume you are inside the above conda environment.

Building the documentation from source
......................................

To build and view the documentation as HTML, execute the following commands:

.. code:: bash

    $ git clone https://github.com/NSSAC/xactor.git
    $ cd xactor
    $ pip install --editable .
    $ pip install -r dev_requirements.txt
    $ make -C docs
    $ <BROWSER-COMMAND> docs/build/html/index.html
