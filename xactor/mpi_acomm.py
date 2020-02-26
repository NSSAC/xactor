"""MPI Async Communication Interface."""

import io
import pickle
import logging
import collections

from mpi4py import MPI

from .evars import get_max_message_size

COMM_WORLD = MPI.COMM_WORLD
WORLD_RANK = COMM_WORLD.Get_rank()
WORLD_SIZE = COMM_WORLD.Get_size()

MAX_MESSAGE_SIZE = get_max_message_size()
MIN_SEND_SIZE = 65536
BUFFER_SIZE = 2 * MAX_MESSAGE_SIZE
NUM_RECV_BUFFERS = 10
MAX_SEND_BUFFERS = 3

DEBUG_FINE = logging.DEBUG - 1
DEBUG_FINER = logging.DEBUG - 2

LOG = logging.getLogger("%s.%d" % (__name__, WORLD_RANK))


def unpickle_buffer(buf):
    """Read objects out of a buffer."""
    reader = io.BytesIO(buf)
    msgs = []
    while True:
        try:
            msg = pickle.load(reader)
            msgs.append(msg)
        except EOFError:
            break
    return msgs


class AsyncRawSender:
    """Manager for sending messages."""

    def __init__(self):
        """Initialize."""
        self.pending_sends = []

    def send(self, to, buf):
        """Send a messge."""
        assert len(buf) <= BUFFER_SIZE

        if __debug__:
            LOG.log(DEBUG_FINE, "Sending %d bytes to %d", len(buf), to)

        req = COMM_WORLD.Isend([buf, MPI.CHAR], dest=to)
        self.pending_sends.append(req)

        if __debug__:
            LOG.log(DEBUG_FINE, "%d send buffers pending", len(self.pending_sends))

        if len(self.pending_sends) < MAX_SEND_BUFFERS:
            indices = MPI.Request.Testsome(self.pending_sends)
        else:
            indices = MPI.Request.Waitsome(self.pending_sends)
        if indices:
            for index in sorted(indices, reverse=True):
                del self.pending_sends[index]

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
        self.buffer = [io.BytesIO() for _ in range(WORLD_SIZE)]
        self.buffer_size = [0 for _ in range(WORLD_SIZE)]
        self.n_messages = [0 for _ in range(WORLD_SIZE)]

    def send(self, to, msg):
        """Send a messge."""
        pickle.dump(msg, self.buffer[to], pickle.HIGHEST_PROTOCOL)

        old_bufsize = self.buffer_size[to]
        new_bufsize = len(self.buffer[to].getbuffer())
        msgsize = new_bufsize - old_bufsize
        if msgsize > MAX_MESSAGE_SIZE:
            raise ValueError("Message too large %d > %d" % (msgsize, MAX_MESSAGE_SIZE))

        self.buffer_size[to] = new_bufsize
        self.n_messages[to] += 1

        if new_bufsize < MIN_SEND_SIZE:
            return

        self.do_flush(to)

    def do_flush(self, to):
        """Send out all buffered messages."""
        buf = self.buffer[to].getbuffer()
        if not buf:
            return

        if __debug__:
            LOG.log(DEBUG_FINE, "Sending %d messages to %d", self.n_messages[to], to)
        self.sender.send(to, buf)

        self.buffer[to] = io.BytesIO()
        self.buffer_size[to] = 0
        self.n_messages[to] = 0

    def flush(self):
        """Flush out message buffers."""
        for to in range(WORLD_SIZE):
            self.do_flush(to)

    def close(self):
        """Flush out any remaining messages and close the sender."""
        self.sender.close()

class AsyncReceiver:
    """Manager for receiving messages."""

    def __init__(self):
        """Initialize."""
        self.bufs = [bytearray(BUFFER_SIZE) for _ in range(NUM_RECV_BUFFERS)]
        self.reqs = [COMM_WORLD.Irecv([buf, MPI.CHAR]) for buf in self.bufs]
        self.stats = [MPI.Status() for _ in self.bufs]

        self.msgq = collections.deque()

    def recv(self):
        """Receive all messages."""
        if self.msgq:
            indices = MPI.Request.Testsome(self.reqs, self.stats)
        else:
            indices = MPI.Request.Waitsome(self.reqs, self.stats)

        if not indices:
            return self.msgq.popleft()

        for idx in sorted(indices):
            status = self.stats[idx]
            frm = status.Get_source()
            cnt = status.Get_count()
            if __debug__:
                LOG.log(DEBUG_FINE, "Received %d bytes from %d", cnt, frm)
            buf = self.bufs[idx]
            buf = buf[:cnt]

            msgs = unpickle_buffer(buf)
            if __debug__:
                LOG.log(DEBUG_FINE, "Received %d messages from %d", len(msgs), frm)
            for msg in msgs:
                self.msgq.append((frm, msg))

        for idx in sorted(indices, reverse=True):
            del self.stats[idx]
            del self.reqs[idx]
            del self.bufs[idx]

        for _ in indices:
            buf = bytearray(BUFFER_SIZE)
            self.bufs.append(buf)
            self.reqs.append(COMM_WORLD.Irecv([buf, MPI.CHAR]))
            self.stats.append(MPI.Status())

        return self.msgq.popleft()

    def close(self):
        """Wait for the receiver thread to end."""
        for req in self.reqs:
            MPI.Request.Cancel(req)

        self.stats.clear()
        self.reqs.clear()
        self.bufs.clear()

class AsyncCommunicator:
    """Communicate with other processes."""

    def __init__(self):
        """Initialize."""
        self.sender = AsyncBufferedSender()
        self.receiver = AsyncReceiver()

        self.flush = self.sender.flush

    def send(self, to, msg):
        """Send a messge."""
        if __debug__:
            LOG.log(DEBUG_FINER, "Sending to %d: %r", to, msg)

        if to == WORLD_RANK:
            self.receiver.msgq.append((WORLD_RANK, msg))
        else:
            self.sender.send(to, msg)

    def recv(self):
        """Receive a message."""
        frm, msg = self.receiver.recv()
        if __debug__:
            LOG.log(DEBUG_FINER, "Received from %d: %r", frm, msg)
        return msg

    def finish(self):
        """Flush the sender and wait for receiver thread to finish."""
        self.sender.flush()
        self.sender.close()

        self.receiver.close()
