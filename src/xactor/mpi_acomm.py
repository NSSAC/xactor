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
    """Read pickled objects out of a buffer.

    Parameters
    ----------
    buf : bytes-like object
        The buffer to read the pickled objects from.

    Returns
    -------
    objs : list of objects
        List of pickled objects from the buffer.
    """
    reader = io.BytesIO(buf)
    objs = []
    while True:
        try:
            obj = pickle.load(reader)
            objs.append(obj)
        except EOFError:
            break
    return objs


class AsyncRawSender:
    """Manager for async send requests.

    Attributes
    ----------
    pending_reqs : list of MPI Request objects
        List of pending send requests
    pending_bufs : list of buffers
        List of buffers corresponding to pending send requests
    """

    def __init__(self):
        """Initialize."""
        self.pending_reqs = []
        self.pending_bufs = []

    def send(self, to, buf):
        """Send a messge.

        Parameters
        ----------
        to : int
            Rank of the destination.
        buf : bytes-like object
            The buffer containing data to send.
        """
        assert len(buf) <= BUFFER_SIZE

        if __debug__:
            LOG.log(DEBUG_FINE, "Sending %d bytes to %d", len(buf), to)

        req = COMM_WORLD.Isend([buf, MPI.BYTE], dest=to, tag=0)
        self.pending_reqs.append(req)
        self.pending_bufs.append(buf)

        if __debug__:
            LOG.log(DEBUG_FINE, "%d send requests in queue", len(self.pending_reqs))

        if len(self.pending_reqs) < MAX_SEND_BUFFERS:
            indices = MPI.Request.Testsome(self.pending_reqs)
        else:
            indices = MPI.Request.Waitsome(self.pending_reqs)
        if indices:
            for index in sorted(indices, reverse=True):
                del self.pending_reqs[index]
                del self.pending_bufs[index]

        if __debug__:
            LOG.log(
                DEBUG_FINE, "%d send requests still pending", len(self.pending_reqs)
            )

    def close(self):
        """Wait for all pending send requests to finish."""
        if not self.pending_reqs:
            return

        MPI.Request.Waitall(self.pending_reqs)
        self.pending_reqs.clear()
        self.pending_bufs.clear()


class AsyncBufferedSender:
    """Manager for sending messages (Python objects).

    Attributes
    ----------
    sender : AsyncRawSender
        The underlying raw sender object used to manage async send requests.
    buffers : list of io.BytesIO
        The BytesIO objects used to manage the buffers for each destination rank.
    n_messages : list of int
        The number of messages contained in each destination buffer.
    """

    def __init__(self):
        """Initialize."""
        self.sender = AsyncRawSender()
        self.buffers = [io.BytesIO() for _ in range(WORLD_SIZE)]
        self.n_messages = [0 for _ in range(WORLD_SIZE)]

    def send(self, to, msg):
        """Send a buffered messge.

        Parameters
        ----------
        to : int
            Rank of the destination.
        msg : object
            The object to be sent.
        """
        old_bufsize = len(self.buffers[to].getbuffer())
        pickle.dump(msg, self.buffers[to], pickle.HIGHEST_PROTOCOL)
        new_bufsize = len(self.buffers[to].getbuffer())
        self.n_messages[to] += 1

        msgsize = new_bufsize - old_bufsize
        if msgsize > MAX_MESSAGE_SIZE:
            raise ValueError("Message too large %d > %d" % (msgsize, MAX_MESSAGE_SIZE))

        if new_bufsize < MIN_SEND_SIZE:
            return
        else:
            self.do_flush(to)

    def do_flush(self, to):
        """Flush the buffer for the destination rank.

        Parameters
        ----------
        to : int
            Rank of the destination.
        """
        buf = self.buffers[to].getbuffer()
        if not buf:
            return

        if __debug__:
            LOG.log(DEBUG_FINE, "Sending %d messages to %d", self.n_messages[to], to)
        self.sender.send(to, buf)

        self.buffers[to] = io.BytesIO()
        self.n_messages[to] = 0

    def flush(self, to=None):
        """Flush out buffers for the destination rank.

        Parameters
        ----------
        to : int or None
            Rank of the destination.
            If to is None then flush out buffers for all destination ranks.
        """
        if to is None:
            for to in range(WORLD_SIZE):
                self.do_flush(to)
        else:
            self.do_flush(to)

    def close(self):
        """Close the underlying sender."""
        self.sender.close()


class AsyncReceiver:
    """Manager for async receive requests.

    Attributes
    ----------
    bufs : list of bytearray
        List of receive buffers
    reqs : list of MPI Request
        List of receive requests
    stats : list of MPI Status
        List of status objects
    msgq : collections.deque
        Queue of received messages
    """

    def __init__(self):
        """Initialize."""
        self.bufs = [bytearray(BUFFER_SIZE) for _ in range(NUM_RECV_BUFFERS)]
        self.reqs = [COMM_WORLD.Irecv([buf, MPI.BYTE], tag=0) for buf in self.bufs]
        self.stats = [MPI.Status() for _ in self.bufs]
        self.msgq = collections.deque()

    def recv(self):
        """Return any received messages.

        If returned message queue is empty
        this method will block until messges are available.

        Returns
        -------
        frm : int
            Rank of the source of the received messsage
        msg : object
            The received message.
        """
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
            self.reqs.append(COMM_WORLD.Irecv([buf, MPI.BYTE], tag=0))
            self.stats.append(MPI.Status())

        return self.msgq.popleft()

    def close(self):
        """Cancel any pending receive requests."""
        for req in self.reqs:
            MPI.Request.Cancel(req)

        self.stats.clear()
        self.reqs.clear()
        self.bufs.clear()


class AsyncCommunicator:
    """Manager of async send and receive requests.

    Attributes
    ----------
    sender : AsyncBufferedSender
        The send buffer manager
    receiver : AsyncReceiver
        The receive buffer manager
    """

    def __init__(self):
        """Initialize."""
        self.sender = AsyncBufferedSender()
        self.receiver = AsyncReceiver()

        self.flush = self.sender.flush

    def send(self, to, msg):
        """Send a messge.

        Parameters
        ----------
        to : int
            Rank of the destination.
        msg : object
            The object to be sent.
        """
        if __debug__:
            LOG.log(DEBUG_FINER, "Sending to %d: %r", to, msg)

        if to == WORLD_RANK:
            self.receiver.msgq.append((WORLD_RANK, msg))
        else:
            self.sender.send(to, msg)

    def recv(self):
        """Wait for and return a received message.

        Returns
        -------
        frm : int
            Rank of the source of the received messsage
        msg : object
            The received message.
        """
        frm, msg = self.receiver.recv()
        if __debug__:
            LOG.log(DEBUG_FINER, "Received from %d: %r", frm, msg)
        return msg

    def finish(self):
        """Finalize the sender and receivers."""
        self.sender.flush()
        self.sender.close()

        self.receiver.close()
