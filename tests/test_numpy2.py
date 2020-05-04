#!/usr/bin/env python3
"""Numpy inverse computing pipeline."""

from time import perf_counter
import numpy as np
import scipy.linalg as linalg

import logging
import xactor as xa

log = xa.getLogger(__name__)

class Worker:
    def __init__(self, source, sink, shape):
        self.source = source
        self.sink = sink
        self.shape = shape

        self.rank = xa.current_rank()

        self.x = np.empty(self.shape, order="C")
        xa.register_buffer(self.x, tag=1)
        self.source.ready(self.rank)

    def invert(self):
        xinv = linalg.inv(self.x)
        xinv = np.array(xinv, order="C", copy=True)
        log.debug("worker xinv:\n%s", xinv)

        prod = np.dot(self.x, xinv)
        assert np.allclose(prod, np.eye(self.shape[0]))
        
        self.sink.send_buffer_(self.x, tag=1)
        self.sink.send_buffer_(xinv, tag=2)
        self.sink.verify()

        self.x = np.empty(self.shape, order="C")
        xa.register_buffer(self.x, tag=1)
        self.source.ready(self.rank)

class Sink:
    def __init__(self, n, shape):
        self.n = n
        self.i = 0
        self.shape = shape
        self.eye = np.eye(shape[0])

        self.start_time = perf_counter()

        self.x = np.empty(self.shape, order="C")
        self.xinv = np.empty(self.shape, order="C")
        xa.register_buffer(self.x, tag=1)
        xa.register_buffer(self.xinv, tag=2)

    def verify(self):
        log.debug("sink xinv:\n%s", self.xinv)

        prod = np.dot(self.x, self.xinv)
        assert np.allclose(prod, self.eye)

        xa.register_buffer(self.x, tag=1)
        xa.register_buffer(self.xinv, tag=2)

        self.i += 1
        if self.i == self.n:
            runtime = perf_counter() - self.start_time
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
            # print(x)
            self.workers[rank].send_buffer_(x, tag=1)
            self.workers[rank].invert()
            self.i += 1

class Main:
    def main(self):
        source_rank = 0
        sink_rank = 1
        shape = (100, 100)

        source = xa.ActorProxy(source_rank, "source")
        sink = xa.ActorProxy(sink_rank, "sink")
        workers = {r: xa.ActorProxy(r, "worker") for r in xa.ranks() if r > 1}

        n = len(workers) * 2

        source.create_actor_(Source, n, shape, workers)
        sink.create_actor_(Sink, n, shape)
        for worker in workers.values():
            worker.create_actor_(Worker, source, sink, shape)

def test_numpy():
    # Create the main actor
    xa.start("main", Main)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG-2)
    test_numpy()
