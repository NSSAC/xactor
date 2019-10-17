"""Simple xactor test."""

from xactor.mpi_actor import Message, get_nodes, get_node_ranks, send, flush, start, stop

class Greeter:
    def __init__(self, name):
        self.name = name

    def greet(self, name):
        print("Greetings to %s from %s" % (name, self.name))

    def main(self):
        for node in get_nodes():
            for rank in get_node_ranks(node):
                rank_id = "rank-%d" % rank
                greeter_id = "greeter-%d" % rank
                msg = Message(rank_id, "create_actor",  greeter_id, Greeter, args=(greeter_id,))
                send(rank, msg, flush=False)
        flush()

        for node in get_nodes():
            for rank in get_node_ranks(node):
                greeter_id = "greeter-%d" % rank
                msg = Message(greeter_id, "greet", "World")
                send(rank, msg, flush=False)
        flush()

        stop()

def test_greeter():
    start(Greeter, "main")
