"""One place to configure logging.

The scripts currently mix 183 `print()` calls with 419 logger calls. Under cron
or in a container the print half bypasses log levels entirely and cannot be
filtered or shipped anywhere, which is precisely when you need it most.

Call `setup_logging()` once at the top of an entry point, then use
`get_logger(__name__)` everywhere else.
"""

from __future__ import annotations

import logging
import os
import sys

__all__ = ["setup_logging", "get_logger"]

_CONSOLE_FORMAT = "%(asctime)s %(levelname)-7s %(name)-28s %(message)s"
_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

_configured = False


def setup_logging(*, level: str | int | None = None, stream=None) -> None:
    """Configure the root logger. Safe to call more than once.

    Level resolution order: the explicit argument, then $LOG_LEVEL, then INFO.

    Logs go to stderr rather than stdout so a script can still write real output
    — a path, a JSON blob — to stdout and stay pipeable.
    """
    global _configured

    resolved = level if level is not None else os.getenv("LOG_LEVEL", "INFO")
    if isinstance(resolved, str):
        resolved = getattr(logging, resolved.upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(resolved)

    if not _configured:
        handler = logging.StreamHandler(stream or sys.stderr)
        handler.setFormatter(logging.Formatter(_CONSOLE_FORMAT, datefmt=_TIME_FORMAT))
        root.addHandler(handler)
        _configured = True

    # boto3 and aiohttp are chatty at DEBUG and drown out everything else.
    for noisy in ("boto3", "botocore", "s3transfer", "urllib3", "aiohttp"):
        logging.getLogger(noisy).setLevel(max(resolved, logging.WARNING))


def get_logger(name: str) -> logging.Logger:
    """Return a namespaced logger, configuring the root one if needed."""
    if not _configured:
        setup_logging()
    return logging.getLogger(name)
