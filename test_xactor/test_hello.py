"""Hello World in XActor."""

import xactor.mpi_actor as xa

class Greeter:
    def greet(self, name):
        print("Greetings to %s from %s" % (xa.current_rank(), name))

class Main:
    def main(self):
        # Create the greeters on very rank
        greeter_id = "greeter"
        for rank in xa.ranks():
            xa.create_actor(rank, greeter_id, Greeter)

        # Send them the greet message
        msg = xa.Message("greet", "world")
        xa.send(xa.EVERY_RANK, greeter_id, msg)

        # we are done now
        xa.stop()

def test_greeter():
    # Create the main actor
    xa.start("main", Main)

if __name__ == "__main__":
    test_greeter()
