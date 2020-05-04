#!/usr/bin/env python3
"""Numpy inverse computing pipeline."""

import sys
from time import perf_counter
import numpy as np
import scipy.linalg as linalg

import logging
import xactor as xa

class Worker:
    def __init__(self, source, sink):
        self.source = source
        self.sink = sink
        self.rank = xa.current_rank()

        self.source.ready(self.rank)

    def invert(self, x):
        xinv = linalg.inv(x)

        self.sink.verify(x, xinv)
        self.source.ready(self.rank)

class Sink:
    def __init__(self, n):
        self.n = n
        self.i = 0
        self._start_time = perf_counter()

    def verify(self, x, xinv):
        prod = np.dot(x, xinv)
        eye = np.eye(x.shape[0])

        assert np.allclose(prod, eye)

        self.i += 1
        if self.i == self.n:
            runtime = perf_counter() - self._start_time
            print("runtime: %e" % runtime)
            xa.stop()

class Source:
    def __init__(self, n, shape, workers):
        self.n = n
        self.shape = shape
        self.workers = workers

        self.i = 0

    def ready(self, rank):
        if self.i < self.n:
            x = np.random.uniform(size=self.shape)
            self.workers[rank].invert(x)
            self.i += 1

class Main:
    def main(self):
        source_rank = 0
        sink_rank = 1
        shape = (100, 100)

        source = xa.ActorProxy(source_rank, "source")
        sink = xa.ActorProxy(sink_rank, "sink")
        workers = {r: xa.ActorProxy(r, "worker") for r in xa.ranks() if r > 1}

        n = len(workers)
        
        source.create_actor_(Source, n, shape, workers)
        sink.create_actor_(Sink, n)
        for worker in workers.values():
            worker.create_actor_(Worker, source, sink)

def main():
    if len(xa.ranks()) < 3:
        print("This program must be run with 3 or more ranks.")
        sys.exit(1)
    xa.start("main", Main)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
