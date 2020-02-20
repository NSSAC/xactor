"""Enironment variable processing."""

import os

PROFILE_DIR_EV = "XACTOR_PROFILE_DIR"
SEND_TRACEBACK_EV = "XACTOR_SEND_TRACEBACK"
MAX_MESSAGE_SIZE_EV = "XACTOR_MAX_MESSAGE_SIZE"


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
    max_message_size = os.environ.get(MAX_MESSAGE_SIZE_EV, "4194304")

    return int(max_message_size)

