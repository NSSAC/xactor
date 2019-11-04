"""Simple xactor test."""

import xactor.mpi_actor as xa

class Greeter:
    def __init__(self, name):
        self.name = name

    def greet(self, name):
        print("Greetings to %s from %s" % (name, self.name))

    def main(self):
        greeter_id = "greeter"

        xa.create_actor(xa.EVERY_RANK, greeter_id, Greeter, greeter_id)

        msg = xa.Message("greet", "world")
        xa.send(xa.EVERY_RANK, greeter_id, msg, immediate=False)
        xa.flush()

        xa.stop()

def test_greeter():
    xa.start("main_actor", Greeter, "main")
