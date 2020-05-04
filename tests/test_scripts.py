"""Test the MPI scripts."""

from pathlib import Path
from subprocess import run

CURDIR = Path(__file__).parent

def mpirun(script, nranks):
    print("Testing '%s' with %d ranks" % (script, nranks))

    script = str(CURDIR / "scripts" / script)
    cmd = ["mpiexec", "-n", str(nranks), "python", script]
    run(cmd, check=True)

def test_hello():
    mpirun("hello.py", 1)
    mpirun("hello.py", 2)
    mpirun("hello.py", 3)

def test_fanout():
    mpirun("fanout.py", 1)
    mpirun("fanout.py", 2)
    mpirun("fanout.py", 3)

def test_numpy():
    mpirun("npbuf.py", 3)
