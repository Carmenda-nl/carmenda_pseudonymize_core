# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Simple job control utilities to allow cancellation of background jobs.

This module provides a JobControl class that manages background job cancellation
using threading.Event objects. Each job is identified by a unique job_id.

This is used exclusively by the API, running the core on its own
will NOT use this.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator

from .logger import setup_logging

logger = setup_logging()


class JobControl:
    """A controller for cooperative cancellation of background jobs."""

    def __init__(self) -> None:
        """Initialize the JobControl instance."""
        self._events: dict[str, threading.Event] = {}
        self._lock = threading.Lock()
        self._local = threading.local()

    @contextmanager
    def run_job(self, job_id: str) -> Generator[None, None, None]:
        """Context manager to run a job with automatic lifecycle management."""
        if getattr(self._local, 'job_id', None) is not None:
            message = f'Thread already has an active job: {self._local.job_id}'
            raise RuntimeError(message)

        with self._lock:
            self._events[job_id] = threading.Event()

        self._local.job_id = job_id
        logger.debug("Job '%s' started", job_id)

        try:
            yield
        finally:
            self._local.job_id = None
            with self._lock:
                event = self._events.pop(job_id, None)
                if event:
                    event.set()

            logger.debug("Job '%s' finished", job_id)

    def cancel(self, job_id: str) -> None:
        """Request cancellation for a job by its ID."""
        with self._lock:
            event = self._events.get(job_id)

            if event:
                event.set()
                logger.info("Job '%s' cancellation requested", job_id)

    def check_cancellation(self) -> None:
        """Check if the current thread's job was cancelled."""
        job_id = getattr(self._local, 'job_id', None)
        if job_id is None:
            return

        with self._lock:
            event = self._events.get(job_id)
            is_cancelled = bool(event and event.is_set())

        if is_cancelled:
            raise JobCancelledError(job_id)

    def is_cancelled(self, job_id: str) -> bool:
        """Check if a specific job was cancelled without raising an exception."""
        with self._lock:
            event = self._events.get(job_id)
            return bool(event and event.is_set())


class JobCancelledError(Exception):
    """Raised when a job is cancelled by a user request."""

    def __init__(self, job_id: str) -> None:
        """Initialize the exception with the job ID."""
        self.job_id = job_id
        super().__init__(f"Job '{job_id}' was cancelled")


job_control = JobControl()
