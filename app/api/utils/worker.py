# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Single-job worker state and execution helpers.

Holds the global Worker instance (tracker + result) and two helpers:
    - run_job: executes process_data on a background thread and writes the result back to worker state
    - shutdown_worker: cancels the running job (if any) and waits for its cleanup to finish
"""

from __future__ import annotations

import contextlib
import dataclasses
import time
from typing import TYPE_CHECKING, Any

from core.processor import process_data
from core.utils.logger import attach_job_log, detach_job_log

if TYPE_CHECKING:
    from core.utils.progress_tracker import ProgressTracker


@dataclasses.dataclass
class Worker:
    """State of this single-process worker."""

    job_id: str | None = None
    tracker: ProgressTracker | None = None
    result: dict[str, Any] | None = None

    @property
    def is_running(self) -> bool:
        """Whether a process is currently being processed (started but no result yet)."""
        return self.tracker is not None and self.result is None


worker = Worker()


def run_job(file: str, input_cols: str, datakey: str, tracker: ProgressTracker, output_dir: str) -> None:
    """Runs process_data on the worker thread and stores the result on the worker state."""
    log_handler = attach_job_log(output_dir)
    status = 'done'

    try:
        result = process_data(file=file, input_cols=input_cols, datakey=datakey, tracker=tracker, output_dir=output_dir)
    except Exception as exc:  # noqa: BLE001 — a failed or cancelled process must free the worker, not crash it.
        status = 'cancelled' if tracker.cancel_requested else 'error'
        result = {'error': 'Process was cancelled' if tracker.cancel_requested else str(exc)}
    finally:
        with contextlib.suppress(Exception):
            tracker.clean_progress_bar()

        tracker.mark_done(status)
        detach_job_log(log_handler)

        worker.result = result


def shutdown_worker() -> None:
    """Cancel the running process (if any) and wait until its cleanup has finished."""
    if worker.tracker is not None:
        worker.tracker.cancel()

    deadline = time.time() + 30
    while worker.is_running and time.time() < deadline:
        time.sleep(0.1)
