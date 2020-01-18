TODO
====

* Write performance test::

    * At every timestep
        * Every actor
            * Computes for a random time period
            * Selects a random message size
            * Selects a random subset of destination nodes
                * Send the message to the destination
    * Run the above for k timesteps

* Debug mode xactor framework profiling
  * Time to pack/unpack

* Allow sending large messages (split across multiple small ones)

* Compress the messages sent with lz4
* Pack messages directly (instead of pickling)
* Use IBcast when broadcasting
* Test Scatterv for sending to multiple nodes

* Implement zeromq backend

* Remove buffering if we use MPI?
* Post one IRecv per rank?

