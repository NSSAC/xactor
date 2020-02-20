"""MPI Async Communication Interface."""

import io
import pickle
import logging
import queue
import threading
import collections

from mpi4py import MPI

from .evars import get_max_message_size

COMM_WORLD = MPI.COMM_WORLD
WORLD_RANK = COMM_WORLD.Get_rank()
WORLD_SIZE = COMM_WORLD.Get_size()

MAX_MESSAGE_SIZE = get_max_message_size()
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
        self.pending_sends = []

    def send(self, to, buf):
        """Send a messge."""
        assert len(buf) <= BUFFER_SIZE

        if __debug__:
            LOG.log(DEBUG_FINER, "Sending %d bytes to %d", len(buf), to)

        req = COMM_WORLD.Isend([buf, MPI.CHAR], dest=to)
        self.pending_sends.append(req)

        indices = MPI.Request.Waitsome(self.pending_sends)
        if indices is not None:
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

        if new_bufsize < MAX_MESSAGE_SIZE:
            return

        self.do_flush(to)

    def do_flush(self, to):
        """Send out all buffered messages."""
        buf = self.buffer[to].getbuffer()
        if not buf:
            return

        if __debug__:
            LOG.log(DEBUG_FINER, "Sending %d messages to %d", self.n_messages[to], to)
        self.sender.send(to, buf)

        self.buffer[to] = io.BytesIO()
        self.buffer_size[to] = 0
        self.n_messages[to] = 0

    def flush(self):
        """Flush out message buffers."""
        for to in range(WORLD_SIZE):
            self.do_flush(to)

    def stop(self):
        """Send stop message to receiver thread."""
        self.sender.send(WORLD_RANK, b"")

    def close(self):
        """Flush out any remaining messages and close the sender."""
        self.sender.close()


class AsyncReceiver:
    """Manager for receiving messages."""

    def __init__(self):
        """Initialize."""
        self.bufq = queue.Queue()
        self.msgq = collections.deque()
        self.finished = threading.Event()

        self._receiver_thread = threading.Thread(target=self._keep_receiving)
        self._receiver_thread.start()

    def recv(self):
        """Receive a message."""
        if self.msgq:
            return self.msgq.popleft()

        if self.finished.is_set():
            raise StopIteration("Receiver was stopped.")

        frm, buf = self.bufq.get(block=True)
        msgs = unpickle_buffer(buf)
        if __debug__:
            LOG.log(DEBUG_FINER, "Received %d messages from %d", len(msgs), frm)
        for msg in msgs:
            self.msgq.append((frm, msg))

        return self.msgq.popleft()

    def join(self):
        """Wait for the receiver thread to end."""
        self._receiver_thread.join()

    def _keep_receiving(self):
        """Code for the receiver thread."""
        while True:
            buf = bytearray(BUFFER_SIZE)
            status = MPI.Status()
            req = COMM_WORLD.Irecv([buf, MPI.CHAR])
            req.Wait(status)
            frm = status.Get_source()
            cnt = status.Get_count()
            if __debug__:
                LOG.log(DEBUG_FINER, "Received %d bytes from %d", cnt, frm)

            if cnt == 0:
                self.finished.set()
                return

            self.bufq.put((frm, buf[:cnt]))


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

        if to == WORLD_RANK:
            self.receiver.msgq.append((WORLD_RANK, msg))
        else:
            self.sender.send(to, msg)

    def recv(self):
        """Receive a message."""
        frm, msg = self.receiver.recv()
        if __debug__:
            LOG.log(DEBUG_FINE, "Received from %d: %r", frm, msg)
        return msg

    def finish(self):
        """Flush the sender and wait for receiver thread to finish."""
        self.sender.flush()
        self.sender.stop()
        self.sender.close()

        self.receiver.join()

        while self.receiver.msgq:
            frm, msg = self.receiver.msgq.popleft()
            LOG.warning("Unretrieved message from %d: %r", frm, msg)

        try:
            while True:
                frm, buf = self.receiver.bufq.get(block=False)
                msgs = unpickle_buffer(buf)
                for msg in msgs:
                    LOG.warning("Unretrieved message from %d: %r", frm, msg)
        except queue.Empty:
            return
