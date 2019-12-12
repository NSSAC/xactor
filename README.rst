XActor: A Distributed Actor Programming Framework
=================================================

XActor is a framework for doing distributed computing in Python,
using the actor model of programming.

Full documentation
------------------

The full documentation is available at ``docs/source/index.rst``

To view the documentation as HTML, compile it as follows::

    $ git clone https://github.com/NSSAC/xactor.git
    $ cd xactor
    $ pip install --editable .
    $ pip install -r dev_requirements.txt
    $ make -C docs
    $ <BROWSER-COMMAND> docs/build/html/index.html
