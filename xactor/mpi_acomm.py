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
HEADER_SIZE = 65536

MAX_MESSAGE_SIZE = BUFFER_SIZE - HEADER_SIZE

DEBUG_FINE = logging.DEBUG - 1
DEBUG_FINER = logging.DEBUG - 2

LOG = logging.getLogger("%s.%d" % (__name__, WORLD_RANK))

# Message used to stop AsyncReceiver
StopAsyncReceiver = sentinel.StopAsyncReceiver
NoMoreMessages = sentinel.NoMoreMessages


class AsyncRawSender:
    """Manager for sending messages."""

    def __init__(self):
        """Initialize."""
        self.pending_sends = []

    def send(self, to, msg):
        """Send a messge."""
        if __debug__:
            LOG.log(DEBUG_FINER, "Sending %d bytes to %d", len(msg), to)

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
        msg_len = len(msg)

        if msg_len > MAX_MESSAGE_SIZE:
            raise ValueError("Message too large %d > %d" % (msg_len, MAX_MESSAGE_SIZE))

        if self.buffer_size[to] + msg_len <= MAX_MESSAGE_SIZE:
            self.buffer[to].append(msg)
            self.buffer_size[to] += msg_len
            return

        if __debug__:
            LOG.log(DEBUG_FINER, "Sending %s messages to %d", len(self.buffer[to]), to)
        buf = pickle.dumps(self.buffer[to], pickle.HIGHEST_PROTOCOL)
        self.sender.send(to, buf)
        self.buffer[to].clear()
        self.buffer_size[to] = 0

        self.buffer[to].append(msg)
        self.buffer_size[to] += msg_len

    def flush(self):
        """Flush out message buffers."""
        for to in range(WORLD_SIZE):
            if self.buffer[to]:
                if __debug__:
                    LOG.log(DEBUG_FINER, "Sending %s messages to %d", len(self.buffer[to]), to)
                buf = pickle.dumps(self.buffer[to], pickle.HIGHEST_PROTOCOL)
                self.sender.send(to, buf)
                self.buffer[to].clear()
                self.buffer_size[to] = 0

    def stop(self):
        """Send stop message to receiver thread."""
        msg = pickle.dumps(StopAsyncReceiver, pickle.HIGHEST_PROTOCOL)
        self.sender.send(WORLD_RANK, msg)

    def close(self):
        """Flush out any remaining messages and close the sender."""
        self.sender.close()


class AsyncReceiver:
    """Manager for receiving messages."""

    def __init__(self):
        """Initialize."""
        self.msgq = queue.Queue()
        self.finished = threading.Event()

        self._receiver_thread = threading.Thread(target=self._keep_receiving)
        self._receiver_thread.start()

    def recv(self):
        """Receive a message."""
        if self.finished.is_set():
            raise StopIteration("Receiver was stopped.")

        return self.msgq.get(block=True)

    def join(self):
        """Wait for the receiver thread to end."""
        self._receiver_thread.join()

    def _keep_receiving(self):
        """Code for the receiver thread."""
        buf = bytearray(BUFFER_SIZE)
        while True:
            status = MPI.Status()
            COMM_WORLD.Irecv([buf, MPI.CHAR]).Wait(status)
            frm = status.Get_source()
            cnt = status.Get_count()
            if __debug__:
                LOG.log(DEBUG_FINER, "Received %d bytes from %d", cnt, frm)

            msgs = pickle.loads(buf[:cnt])
            if msgs is StopAsyncReceiver:
                self.finished.set()
                return

            if __debug__:
                LOG.log(DEBUG_FINER, "Received %d messages from %d", len(msgs), frm)
            for msg in msgs:
                self.msgq.put((frm, msg))


class AsyncCommunicator:
    """Communicate with other processes."""

    def __init__(self):
        """Initialize."""
        self.sender = AsyncBufferedSender()
        self.receiver = AsyncReceiver()

        self.flush = self.sender.flush
        self.join = self.receiver.join

    def send(self, to, msg):
        """Send a messge."""
        if __debug__:
            LOG.log(DEBUG_FINE, "Sending to %d: %r", to, msg)

        msg = pickle.dumps(msg, pickle.HIGHEST_PROTOCOL)
        if to == WORLD_RANK:
            self.receiver.msgq.put((WORLD_RANK, msg))
        else:
            self.sender.send(to, msg)

    def recv(self):
        """Receive a message."""
        frm, msg = self.receiver.recv()
        msg = pickle.loads(msg)
        if __debug__:
            LOG.log(DEBUG_FINE, "Received from %d: %r", frm, msg)
        return msg

    def finish(self):
        """Flush the sender and wait for receiver thread to finish."""
        self.sender.flush()
        self.sender.stop()
        self.sender.close()

        self.receiver.join()

        qsize = self.receiver.msgq.qsize()
        if qsize:
            LOG.warning(
                "Communicator finished with %d messages still in receiver queue", qsize
            )
