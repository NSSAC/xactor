"""Hello World in XActor."""

from time import time

import random
import logging
import xactor as xa

MIN_NUM = 100
MAX_NUM = 200
MIN_SIZE = 100
MAX_SIZE = 200

class Consumer:
    def __init__(self):
        self.objects_received = 0
        self.main = xa.ActorProxy(xa.MASTER_RANK, "main")

    def consume(self, msg):
        #print("%d Received %d objects" % (xa.current_rank(), len(msg)))
        self.objects_received += len(msg)

    def producer_done(self):
        self.main.consumer_done(self.objects_received, send_immediate=True)

class Producer:
    def __init__(self):
        self.main = xa.ActorProxy(xa.MASTER_RANK, "main")
        self.consumer = [xa.ActorProxy(rank, "consumer") for rank in xa.ranks()]
        self.every_consumer = xa.ActorProxy(xa.EVERY_RANK, "consumer")

    def produce(self):
        objects_sent = 0

        n_messages = random.randint(MIN_NUM, MAX_NUM)
        for _ in range(n_messages):
            rank = random.choice(xa.ranks())

            msg_size = random.randint(MIN_SIZE, MAX_SIZE)
            objects_sent += msg_size

            msg = list(range(msg_size))
            self.consumer[rank].consume(msg)
            #print("Sent %d objects to %d" % (len(msg), rank))

        self.every_consumer.producer_done()
        self.main.producer_done(objects_sent, send_immediate=True)

class Main:
    def __init__(self):
        self.start = None
        self.end = None
        self.objects_sent = 0
        self.objects_received = 0
        self.num_consumer_done = 0
        self.producer = xa.ActorProxy(xa.MASTER_RANK, "producer")

    def main(self):
        xa.create_actor(xa.MASTER_RANK, "producer", Producer)
        xa.create_actor(xa.EVERY_RANK, "consumer", Consumer)

        self.start = time()
        self.producer.produce(send_immediate=True)

    def producer_done(self, n):
        self.objects_sent = n
        self.maybe_stop()

    def consumer_done(self, n):
        self.objects_received += n
        self.num_consumer_done += 1
        self.maybe_stop()

    def maybe_stop(self):
        if self.num_consumer_done == len(xa.ranks()):
            self.end = time()

            print("n_sent: %d" % self.objects_sent)
            print("n_received: %d" % self.objects_received)

            print("n_ranks: %d" % len(xa.ranks()))
            print("n_nodes: %d" % len(xa.nodes()))

            runtime = (self.end - self.start)
            print("runtime: %e" % runtime)

            xa.stop()

def test_fanout():
    xa.start("main", Main)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_fanout()
