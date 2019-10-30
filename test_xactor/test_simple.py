"""Simple xactor test."""

import xactor.mpi_actor as xa

class Greeter:
    def __init__(self, name):
        self.name = name

    def greet(self, name):
        print("Greetings to %s from %s" % (name, self.name))

    def main(self):
        for node in xa.nodes():
            for rank in xa.node_ranks(node):
                greeter_id = "greeter-%d" % rank
                msg = xa.Message(xa.RANK_ACTOR_ID, "create_actor",  greeter_id, Greeter, args=(greeter_id,))
                xa.send(rank, msg)
        xa.flush()

        for node in xa.nodes():
            for rank in xa.node_ranks(node):
                greeter_id = "greeter-%d" % rank
                msg = xa.Message(greeter_id, "greet", "world")
                xa.send(rank, msg, flush=False)
        xa.flush()

        xa.stop()

def test_greeter():
    xa.start("main_actor", Greeter, "main")
