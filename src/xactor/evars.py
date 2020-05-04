"""Enironment variable processing.

The following environment variables can be used
to configure XActor's runtime behavior.

Debug mode configuration variables
----------------------------------

The following two environment variables are only relevant
if Python is not running in optimized mode that is with the -O flag.

**XACTOR_PROFILE_DIR:**
  | If defined XActor will profile the actor method calls
    and will write the profiling results *at the end of the program*
    in files in the given directory.
    One file will be created per rank.
  | Example usage: ``export XACTOR_PROFILE_DIR=/tmp``

**XACTOR_SEND_TRACEBACK:**
  | If defined XActor will send the sender traceback along with messages.
    The traceback is printed if there is a problem at the receiver
    when trying to execute the called method on the actor.
  | Example usage: ``export XACTOR_SEND_TRACEBACK=1``

Runtime configuration variables
-------------------------------

**XACTOR_MAX_MESSAGE_SIZE:**
  | Maximum expected size of a single message sent using XActor.
    Note this is the size, in bytes, of the result of pickling the message.
  | Default: 4194304 (4MB)
  | Example usage: ``export XACTOR_MAX_MESSAGE_SIZE=4194304``

**XACTOR_MIN_SEND_SIZE:**
  | Send buffers will be flushed once it is full beyond this size.
  | Default: 65536 (64KB)
  | Example usage: ``export XACTOR_MIN_SEND_SIZE=65536``

**XACTOR_NUM_RECV_BUFFERS:**
  | Number of receive buffers to post (on each rank) at a time.
  | Default: 1
  | Example usage: ``export XACTOR_NUM_RECV_BUFFERS=1``

**XACTOR_MAX_SEND_BUFFERS:**
  | Maximum number of send buffers (on each rank) at a time.
    XActor's `send` method will block once this number is reached.
  | Default: 2
  | Example usage: ``export XACTOR_MAX_SEND_BUFFERS=2``

"""

import os

PROFILE_DIR_EV = "XACTOR_PROFILE_DIR"
SEND_TRACEBACK_EV = "XACTOR_SEND_TRACEBACK"

MAX_MESSAGE_SIZE_EV = "XACTOR_MAX_MESSAGE_SIZE"
MIN_SEND_SIZE_EV = "XACTOR_MIN_SEND_SIZE"
NUM_RECV_BUFFERS_EV = "XACTOR_NUM_RECV_BUFFERS"
MAX_SEND_BUFFERS_EV = "XACTOR_MAX_SEND_BUFFERS"


def get_profile_dir():
    """Return the directory to save runtime profiles."""
    profile_dir = os.environ.get(PROFILE_DIR_EV, None)
    if profile_dir is None:
        return None

    if profile_dir is not None:
        if not os.path.isdir(profile_dir):
            raise ValueError(
                f"Environment variable {PROFILE_DIR_EV} points to '{profile_dir}' which is not a directory"
            )

    return profile_dir


def do_send_traceback():
    """Return true if sender traceback should be sent with messages."""
    send_traceback = os.environ.get(SEND_TRACEBACK_EV, None)
    if send_traceback is None:
        return False

    return bool(int(send_traceback))


def get_max_message_size():
    """Return the max message size."""
    evar = os.environ.get(MAX_MESSAGE_SIZE_EV, "4194304")
    return int(evar)


def get_min_send_size():
    """Return the minimum send size."""
    evar = os.environ.get(MIN_SEND_SIZE_EV, "65536")
    return int(evar)


def get_num_recv_buffers():
    """Return the number of recv buffers to post."""
    evar = os.environ.get(NUM_RECV_BUFFERS_EV, "1")
    return int(evar)


def get_max_send_buffers():
    """Return the maximum number of send buffers at a time."""
    evar = os.environ.get(MAX_SEND_BUFFERS_EV, "2")
    return int(evar)
