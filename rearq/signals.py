import signal
from typing import Callable

_sig_handles = set()


def add_sig_handler(handler: Callable):
    _sig_handles.add(handler)


def handle_sign(signum: int, _):
    for h in _sig_handles:
        h(signum)


signal.signal(signal.SIGINT, handle_sign)
signal.signal(signal.SIGTERM, handle_sign)
