"""MPI Async Communication Interface."""

import os
import pickle
import logging
import queue
import threading
from unittest.mock import sentinel

from mpi4py import MPI

COMM_WORLD = MPI.COMM_WORLD
WORLD_RANK = COMM_WORLD.Get_rank()
WORLD_SIZE = COMM_WORLD.Get_size()

if "XACTOR_BUFFER_SIZE" in os.environ:
    BUFFER_SIZE = int(os.environ["XACTOR_BUFFER_SIZE"])
else:
    BUFFER_SIZE = 4194304  # 4MB
HEADER_SIZE = 4096

MAX_MESSAGE_SIZE = BUFFER_SIZE - HEADER_SIZE

LOG = logging.getLogger("%s.%d" % (__name__, WORLD_RANK))

# Message used to stop AsyncReceiver
StopAsyncReceiver = sentinel.StopAsyncReceiver


class AsyncRawSender:
    """Manager for sending messages."""

    def __init__(self):
        """Initialize."""
        self.pending_sends = []

    def send(self, to, msg):
        """Send a messge."""
        req = COMM_WORLD.Isend([msg, MPI.CHAR], dest=to)
        self.pending_sends.append(req)

        # Cleanup send requests that have already completed.
        indices = MPI.Request.Waitsome(self.pending_sends)
        if indices is None:
            return

        indices = set(indices)
        self.pending_sends = [
            r for i, r in enumerate(self.pending_sends) if i not in indices
        ]

    def close(self):
        """Wait for all pending send requests to finish."""
        if not self.pending_sends:
            return

        MPI.Request.Waitall(self.pending_sends)
        self.pending_sends.clear()


class AsyncBufferedSender:
    """Manager for sending messages."""

    def __init__(self):
        """Initialize."""
        self.sender = AsyncRawSender()
        self.buffer = [[] for _ in range(WORLD_SIZE)]
        self.buffer_size = [0 for _ in range(WORLD_SIZE)]

    def send(self, to, msg):
        """Send a messge."""
        pkl = pickle.dumps(msg, pickle.HIGHEST_PROTOCOL)
        pkl_len = len(pkl)
        if pkl_len > MAX_MESSAGE_SIZE:
            raise ValueError("Message too large %d > %d" % (pkl_len, MAX_MESSAGE_SIZE))

        if self.buffer_size[to] + pkl_len <= MAX_MESSAGE_SIZE:
            self.buffer[to].append(pkl)
            self.buffer_size[to] += pkl_len
            return

        if __debug__:
            LOG.debug("Sending %s messages to %d", len(self.buffer[to]), to)
        buf = pickle.dumps(self.buffer[to], pickle.HIGHEST_PROTOCOL)
        self.sender.send(to, buf)
        self.buffer[to].clear()
        self.buffer_size[to] = 0

        self.buffer[to].append(pkl)
        self.buffer_size[to] += pkl_len

    def flush(self):
        """Flush out message buffers."""
        for to in range(WORLD_SIZE):
            if self.buffer[to]:
                if __debug__:
                    LOG.debug("Sending %s messages to %d", len(self.buffer[to]), to)
                buf = pickle.dumps(self.buffer[to], pickle.HIGHEST_PROTOCOL)
                self.sender.send(to, buf)
                self.buffer[to].clear()
                self.buffer_size[to] = 0

    def close(self):
        """Flush out any remaining messages and close the sender."""
        self.flush()
        self.sender.close()


class AsyncReceiver:
    """Manager for receiving messages."""

    def __init__(self):
        """Initialize."""
        self.msgq = queue.Queue()

        self._buf = bytearray(BUFFER_SIZE)
        self._receiver_thread = threading.Thread(target=self._keep_receiving)
        self._receiver_thread.start()

    def recv(self, block=True):
        """Receive a message."""
        return self.msgq.get(block=block)

    def join(self):
        """Wait for the receiver thread to end."""
        self._receiver_thread.join()

    def _keep_receiving(self):
        """Code for the receiver thread."""
        stop_receiver = False
        while not stop_receiver:
            COMM_WORLD.Irecv([self._buf, MPI.CHAR]).Wait()
            msgs = pickle.loads(self._buf)
            if __debug__:
                LOG.debug("Received %d messages", len(msgs))
            for msg in msgs:
                msg = pickle.loads(msg)
                if msg is StopAsyncReceiver:
                    stop_receiver = True
                    continue

                self.msgq.put(msg)


class AsyncCommunicator:
    """Communicate with other processes."""

    def __init__(self):
        """Initialize."""
        self.sender = AsyncBufferedSender()
        self.receiver = AsyncReceiver()

        self.flush = self.sender.flush

        self.recv = self.receiver.recv
        self.join = self.receiver.join

    def send(self, to, msg):
        """Send a messge."""
        if to == WORLD_RANK:
            self.receiver.msgq.put(msg)
        else:
            self.sender.send(to, msg)

    def finish(self):
        """Flush the sender and wait for receiver thread to finish."""
        self.sender.send(WORLD_RANK, StopAsyncReceiver)
        self.sender.close()
        self.receiver.join()

        qsize = self.receiver.msgq.qsize()
        if qsize:
            LOG.warning(
                "Communicator finished with %d messages still in receiver queue", qsize
            )
