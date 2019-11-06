"""Hello World in XActor."""

import random
import xactor.mpi_actor as xa

MIN_NUM = 100
MAX_NUM = 200
MIN_SIZE = 100
MAX_SIZE = 200

class Consumer:
    def __init__(self):
        self.objects_received = 0

    def consume(self, msg):
        print("%d Received %d objects" % (xa.current_rank(), len(msg)))
        self.objects_received += len(msg)

    def producer_done(self):
        xa.send(xa.MASTER_RANK, "main", xa.Message("consumer_done", self.objects_received))

class Producer:
    def produce(self):
        objects_sent = 0

        n_messages = random.randint(MIN_NUM, MAX_NUM)
        for _ in range(n_messages):
            rank = random.choice(xa.ranks())

            msg_size = random.randint(MIN_SIZE, MAX_SIZE)
            objects_sent += msg_size

            msg = list(range(msg_size))
            xa.send(rank, "consumer", xa.Message("consume", msg))
            print("Sent %d objects to %d" % (len(msg), rank))

        xa.send(xa.EVERY_RANK, "consumer", xa.Message("producer_done"))
        xa.send(xa.MASTER_RANK, "main", xa.Message("producer_done", objects_sent))

class Main:
    def __init__(self):
        self.objects_sent = 0
        self.objects_received = 0

    def main(self):
        xa.create_actor(xa.MASTER_RANK, "producer", Producer)
        xa.create_actor(xa.EVERY_RANK, "consumer", Consumer)

        msg = xa.Message("produce")
        xa.send(xa.MASTER_RANK, "producer", msg)

    def maybe_stop(self):
        if self.objects_sent != 0 and self.objects_sent == self.objects_received:
            print("Sent %d, Received %d" % (self.objects_sent, self.objects_received))
            xa.stop()

    def producer_done(self, n):
        self.objects_sent = n
        self.maybe_stop()

    def consumer_done(self, n):
        self.objects_received += n
        self.maybe_stop()

def test_greeter():
    xa.start("main", Main)

if __name__ == "__main__":
    test_greeter()
