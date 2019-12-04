# TODO

* Write performance test
    * At every timestep
        * Every actor
            * Computes for a random time period
            * Select a random message size
            * Select a random subset of destination nodes
                * Send the message to the destination
    * Run the above for k timesteps

* Allow sending large messages (split across multiple small ones)

* Compress the messages sent with lz4
* Pack messages directly (instead of pickling)
* Post one IRecv per rank
* Use IBcast when broadcasting
* Test Scatterv for sending to multiple nodes

* Implement python multiprocessing backend
* Implement zeromq backend
