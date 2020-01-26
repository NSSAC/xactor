"""Test latency.

Compute the average time to send one "message"
from one rank to another.
"""

import sys
import logging
import statistics as st
from time import perf_counter

import xactor as xa

MASTER_RANK = 0
WORKER_RANK = 1

RUNTIME = 30  # seconds


class Worker:
    def __init__(self):
        self.main_actor = xa.ActorProxy(MASTER_RANK, "main")

    def ping(self):
        self.main_actor.pong(send_immediate=True)


class Main:
    def __init__(self):
        self.worker = xa.ActorProxy(WORKER_RANK, "worker")

        self.prog_start = perf_counter()
        self.ping_start = None
        self.timings = []

    def main(self):
        xa.create_actor(WORKER_RANK, "worker", Worker)

        self.ping_start = perf_counter()
        self.worker.ping(send_immediate=True)

    def pong(self):
        time_taken = (perf_counter() - self.ping_start) / 2
        self.timings.append(time_taken)

        if perf_counter() - self.prog_start > RUNTIME:
            print("mean: %e" % st.mean(self.timings))
            print("variance: %e" % st.variance(self.timings))
            print("max: %e" % max(self.timings))
            print("min: %e" % min(self.timings))
            print("n: %d" % len(self.timings))
            print("runtime: %.3f" % (perf_counter() - self.prog_start))

            xa.stop()
            return

        self.ping_start = perf_counter()
        self.worker.ping(send_immediate=True)


def test_latency():
    if len(xa.ranks()) != 2:
        print("This program must be run with exactly 2 ranks.")
        sys.exit(1)
    xa.start("main", Main)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_latency()
