#!/usr/bin/env python3
"""Hello World in XActor."""

import logging
import xactor as xa

# This is the class of the `Greeter` actor.
class Greeter:
    def greet(self, name):
        print("Greetings to %s from rank %s" % (name, xa.current_rank()))

# This the class of the `Main` actor.
class Main:
    def main(self):
        greeter_id = "greeter"

        # Create the actor proxy
        every_greeter = xa.ActorProxy(xa.EVERY_RANK, greeter_id)

        # Create the actors
        every_greeter.create_actor(Greeter)

        # Send the greeters the greet message.
        every_greeter.greet("world", send_immediate=True)

        # We are done now.
        xa.stop()

def test_greeter():
    # Create the main actor
    xa.start("main", Main)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_greeter()
