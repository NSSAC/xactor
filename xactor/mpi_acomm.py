"""MPI Async Communication Interface."""

import pickle
import logging
import queue
import threading
from unittest.mock import sentinel

from mpi4py import MPI

COMM_WORLD = MPI.COMM_WORLD
WORLD_RANK = COMM_WORLD.Get_rank()
WORLD_SIZE = COMM_WORLD.Get_size()

BUFFER_SIZE = 4194304  # 4MB
HEADER_SIZE = 4096

MAX_MESSAGE_SIZE = BUFFER_SIZE - HEADER_SIZE

LOG = logging.getLogger("%s.%d" % (__name__, WORLD_RANK))

# Message used to stop AsyncReceiver
StopAsyncReceiver = sentinel.StopAsyncReceiver


class MessageBuffer:
    """Buffer for pickling objects."""

    def __init__(self):
        self.buffer = []
        self.total_size = 0

    def append(self, msg):
        """Add an object to the buffer."""
        pkl = pickle.dumps(msg, pickle.HIGHEST_PROTOCOL)
        pkl_len = len(pkl)
        if pkl_len > MAX_MESSAGE_SIZE:
            raise ValueError("Message too large %d > %d" % (pkl_len, MAX_MESSAGE_SIZE))

        if self.total_size + pkl_len < MAX_MESSAGE_SIZE:
            self.buffer.append(pkl)
            self.total_size += pkl_len
            return None

        imsgs = self.buffer
        self.buffer = [pkl]
        self.total_size = pkl_len
        return imsgs

    def flush(self):
        """Flush out the current buffer."""
        if not self.buffer:
            return None

        imsgs = self.buffer
        self.buffer = []
        self.total_size = 0
        return imsgs


class AsyncSender:
    """Manager for sending messages."""

    def __init__(self):
        """Initialize."""
        self.buffer = {rank: MessageBuffer() for rank in range(WORLD_SIZE)}
        self.pending_sends = []

    def send(self, to, msg):
        """Send a messge."""
        imsgs = self.buffer[to].append(msg)
        if imsgs is not None:
            self.do_send(to, imsgs)
            self.cleanup_finished_sends()

    def do_send(self, to, msgs):
        """Send all messages that have been cached."""
        pkl = pickle.dumps(msgs, pickle.HIGHEST_PROTOCOL)
        assert len(pkl) <= BUFFER_SIZE
        if __debug__:
            LOG.debug("Sending %d messages to %d", len(msgs), to)

        req = COMM_WORLD.Isend([pkl, MPI.CHAR], dest=to)
        self.pending_sends.append(req)

    def flush(self, wait=True):
        """Flush out message buffers."""
        for rank in range(WORLD_SIZE):
            msgs = self.buffer[rank].flush()
            if msgs is not None:
                self.do_send(rank, msgs)

        if wait:
            self.wait_pending_sends()
        else:
            self.cleanup_finished_sends()

    def cleanup_finished_sends(self):
        """Cleanup send requests that have already completed."""
        if not self.pending_sends:
            return

        indices = MPI.Request.Waitsome(self.pending_sends)
        if indices is None:
            return

        indices = set(indices)
        self.pending_sends = [
            r for i, r in enumerate(self.pending_sends) if i not in indices
        ]

    def wait_pending_sends(self):
        """Wait for all pending send requests to finish."""
        if not self.pending_sends:
            return

        MPI.Request.Waitall(self.pending_sends)
        self.pending_sends.clear()


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
        self.sender = AsyncSender()
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
        self.sender.flush()
        self.receiver.join()

        qsize = self.receiver.msgq.qsize()
        if qsize:
            LOG.warning(
                "Communicator finished with %d messages still in receiver queue", qsize
            )
