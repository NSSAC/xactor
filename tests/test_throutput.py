"""Test througput.

Compute average messages per second
from one rank to another.
"""

import sys
import logging
from time import perf_counter

import xactor as xa

MASTER_RANK = 0
WORKER_RANK = 1

RUNTIME = 30  # seconds


class Worker:
    def __init__(self):
        self.main_actor = xa.ActorProxy(MASTER_RANK, "main")
        self.prog_start = perf_counter()

        self.start_time = None
        self.end_time = None
        self.num_messages = 0

    def start(self, start_time):
        self.start_time = start_time

    def ping(self):
        self.num_messages += 1

    def stop(self, end_time):
        self.end_time = end_time

        time_taken = self.end_time - self.start_time
        mps = self.num_messages / time_taken

        print("mps: %e" % mps)
        print("n: %d" % self.num_messages)
        print("runtime: %.3f" % (perf_counter() - self.prog_start))
        xa.stop()


class Main:
    def __init__(self):
        self.worker = xa.ActorProxy(WORKER_RANK, "worker")

    def main(self):
        xa.create_actor(WORKER_RANK, "worker", Worker)

        start_time = perf_counter()
        self.worker.start(start_time)

        while True:
            cur_time = perf_counter()
            elapsed = cur_time - start_time
            if elapsed > RUNTIME:
                self.worker.stop(cur_time, send_immediate=True)
                return

            self.worker.ping()
            #self.worker.ping(send_immediate=True)


def test_througput():
    if len(xa.ranks()) != 2:
        print("This program must be run with exactly 2 ranks.")
        sys.exit(1)
    xa.start("main", Main)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_througput()
