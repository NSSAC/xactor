"""MPI Async Communication Interface."""

import io
import pickle
import logging
import collections

from mpi4py import MPI

from .evars import (
    get_max_message_size,
    get_min_send_size,
    get_num_recv_buffers,
    get_max_send_buffers,
)

COMM_WORLD = MPI.COMM_WORLD
WORLD_RANK = COMM_WORLD.Get_rank()
WORLD_SIZE = COMM_WORLD.Get_size()

MAX_MESSAGE_SIZE = get_max_message_size()
MIN_SEND_SIZE = get_min_send_size()
NUM_RECV_BUFFERS = get_num_recv_buffers()
MAX_SEND_BUFFERS = get_max_send_buffers()

BUFFER_SIZE = 2 * MAX_MESSAGE_SIZE

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
        self.reqs = []
        self.bufs = []

    def send(self, to, buf, tag):
        """Send a messge."""
        assert len(buf) <= BUFFER_SIZE

        if __debug__:
            LOG.log(DEBUG_FINE, "Sending %d bytes to %d with tag = %d", len(buf), to, tag)

        req = COMM_WORLD.Isend(buf, dest=to, tag=tag)
        self.bufs.append(buf)
        self.reqs.append(req)

        if __debug__:
            LOG.log(DEBUG_FINE, "%d send buffers pending", len(self.reqs))

        if len(self.reqs) < MAX_SEND_BUFFERS:
            indices = MPI.Request.Testsome(self.reqs)
        else:
            indices = MPI.Request.Waitsome(self.reqs)

        if indices:
            for index in sorted(indices, reverse=True):
                del self.reqs[index]
                del self.bufs[index]

    def close(self):
        """Wait for all pending send requests to finish."""
        if not self.reqs:
            return

        MPI.Request.Waitall(self.reqs)
        self.reqs.clear()
        self.bufs.clear()


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
        self.sender.send(to, buf, tag=0)

        self.buffer[to] = io.BytesIO()
        self.buffer_size[to] = 0
        self.n_messages[to] = 0

    def flush(self, to=None):
        """Flush out message buffers."""
        if to is None:
            for to in range(WORLD_SIZE):
                self.do_flush(to)
        else:
            self.do_flush(to)

    def close(self):
        """Flush out any remaining messages and close the sender."""
        self.sender.close()


class AsyncReceiver:
    """Manager for receiving messages."""

    def __init__(self):
        """Initialize."""
        self.bufs = [bytearray(BUFFER_SIZE) for _ in range(NUM_RECV_BUFFERS)]
        self.reqs = [COMM_WORLD.Irecv(buf, tag=0) for buf in self.bufs]
        self.stats = [MPI.Status() for _ in self.bufs]

        self.msgq = collections.deque()

    def register_buffer(self, buf, tag):
        """Register a buffer for receiveing."""
        assert tag > 0, "Custom buffers can only be received with tag > 0"

        self.bufs.append(buf)
        self.reqs.append(COMM_WORLD.Irecv(buf, tag=tag))
        self.stats.append(MPI.Status())

    def recv(self):
        """Receive all messages."""
        while True:
            # If msgq has something, we dont wait
            if self.msgq:
                indices = MPI.Request.Testsome(self.reqs, self.stats)
            else:
                indices = MPI.Request.Waitsome(self.reqs, self.stats)

            # If we dont have any incoming messages
            # but we got to this point in code
            # then msgq must have messages
            if not indices:
                return self.msgq.popleft()

            # Process the recvs
            num_message_buffers = 0
            for idx in sorted(indices):
                status = self.stats[idx]
                frm = status.Get_source()
                cnt = status.Get_count()
                tag = status.Get_tag()
                if __debug__:
                    LOG.log(DEBUG_FINE, "Received %d bytes from %d with tag %d", cnt, frm, tag)
                buf = self.bufs[idx]

                # If tag = 0
                # We need to unpickle the messages
                if tag == 0:
                    num_message_buffers += 1

                    buf = buf[:cnt]
                    msgs = unpickle_buffer(buf)
                    if __debug__:
                        LOG.log(DEBUG_FINE, "Received %d messages from %d", len(msgs), frm)
                    for msg in msgs:
                        self.msgq.append((frm, msg))

            # Delete the already used up stats, bufs, and reqs
            for idx in sorted(indices, reverse=True):
                del self.stats[idx]
                del self.reqs[idx]
                del self.bufs[idx]

            # Recreate the consumed message buffers
            for _ in range(num_message_buffers):
                buf = bytearray(BUFFER_SIZE)
                self.bufs.append(buf)
                self.reqs.append(COMM_WORLD.Irecv(buf, tag=0))
                self.stats.append(MPI.Status())

            # If msgq is not empty
            # return an element from the queue
            if self.msgq:
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

        self.send_buffer = self.sender.sender.send
        self.register_buffer = self.receiver.register_buffer

    def send(self, to, msg):
        """Send a messge."""
        if __debug__:
            LOG.log(DEBUG_FINER, "Sending to %d: %r", to, msg)

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
