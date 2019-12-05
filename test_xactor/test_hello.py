"""Hello World in XActor."""

import logging
import xactor as xa

class Greeter:
    def greet(self, name):
        print("Greetings to %s from %s" % (xa.current_rank(), name))
        raise ValueError()

class Main:
    def main(self):
        # Create the greeters on very rank
        greeter_id = "greeter"
        for rank in xa.ranks():
            xa.create_actor(rank, greeter_id, Greeter)

        # Send them the greet message
        every_greeter = xa.ActorProxy(xa.EVERY_RANK, greeter_id)
        every_greeter.greet("world", send_immediate=True)

        # we are done now
        xa.stop()

def test_greeter():
    # Create the main actor
    xa.start("main", Main)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_greeter()
