"""Enironment variable processing."""

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
    evar = os.environ.get(NUM_RECV_BUFFERS_EV, "10")
    return int(evar)


def get_max_send_buffers():
    """Return the maximum number of send buffers at a time."""
    evar = os.environ.get(MAX_SEND_BUFFERS_EV, "3")
    return int(evar)
