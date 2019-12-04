"""Ping pong test."""

from time import time

import logging
import xactor as xa

class Worker:
    def __init__(self):
        self.main_actor = xa.ActorProxy(xa.MASTER_RANK, "main")

    def ping(self):
        self.main_actor.pong(send_immediate=True)

class Main:
    def __init__(self):
        self.start = None
        self.end = None
        self.workers_done = 0
        self.every_worker = xa.ActorProxy(xa.EVERY_RANK, "worker")

    def main(self):
        xa.create_actor(xa.EVERY_RANK, "worker", Worker)

        self.start = time()
        self.every_worker.ping(send_immediate=True)

    def pong(self):
        self.workers_done += 1
        if self.workers_done == xa.WORLD_SIZE:
            self.end = time()

            print("n_ranks: %d" % xa.WORLD_SIZE)
            print("n_nodes: %d" % len(xa.nodes()))

            runtime = (self.end - self.start) / 1e-6
            print("runtime: %g us" % runtime)

            xa.stop()

def test_ping_pong():
    xa.start("main", Main)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_ping_pong()
