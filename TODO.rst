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

* Compress the messages sent with lz4
* Use IBcast when broadcasting
* Test Scatterv for sending to multiple nodes
