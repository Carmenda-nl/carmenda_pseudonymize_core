# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Progress tracking utilities for data processing.

This module provides the ProgressTracker for managing and reporting
progress during data transformation using Rich library.
"""

from __future__ import annotations

import io
import sys
import time
from datetime import timedelta

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from .logger import setup_logging

logger = setup_logging()


class JobCancelledError(Exception):
    """Raised inside the processing thread when the process has been cancelled.

    The message parameter is required because polars re-creates the exception
    with a message argument when it propagates out of a map_batches UDF.
    """

    def __init__(self, message: str = 'Process was cancelled') -> None:
        """Initialize the exception with a default message."""
        super().__init__(message)


class ProgressTracker:
    """Track progress for data transformation using Rich.

    Instance variables are read by the FastAPI event loop and written by
    the processing thread. Python's GIL makes simple reads/writes safe.
    The cancel flag works the other way around: the event loop sets it and
    the processing thread raises JobCancelledError at its next checkpoint.
    """

    def __init__(self) -> None:
        """Initialize the progress tracker."""
        self.stage: str | None = None
        self.percentage: int = 0
        self.rows_total: int | None = None
        self.rows_processed: int | None = None
        self.cancel_requested = False
        self.task_id: TaskID | None = None
        self.rich_progress: Progress | None = None
        self.rows_progress = 0

    def _progress_bar(self) -> Progress:
        """Create a Rich progress bar with spinner."""
        spinner = SpinnerColumn()
        text = TextColumn('[bold blue]{task.description}', justify='left')
        bar = BarColumn(bar_width=40)
        task_progress = TaskProgressColumn()
        mofn = MofNCompleteColumn()
        time_elapsed = TimeElapsedColumn()
        time_remaining = TimeRemainingColumn()

        # Disable console output when running as PyInstaller executable (prevents Unicode errors)
        if getattr(sys, 'frozen', False):
            disable_console = Console(file=io.StringIO(), force_terminal=False)

            return Progress(
                spinner,
                text,
                bar,
                task_progress,
                mofn,
                time_elapsed,
                time_remaining,
                console=disable_console,
                disable=False,
            )

        return Progress(spinner, text, bar, task_progress, mofn, time_elapsed, time_remaining)

    def clean_progress_bar(self) -> None:
        """Stop the Rich progress bar and clean up resources."""
        if self.rich_progress is not None:
            self.rich_progress.stop()
            sys.stdout.write('\n')

        self.rich_progress = None
        self.task_id = None

    def cancel(self) -> None:
        """Request cancellation; the processing thread aborts at its next checkpoint."""
        self.cancel_requested = True

    def check_cancelled(self) -> None:
        """Raise JobCancelledError when cancellation was requested. Called from the processing thread."""
        if self.cancel_requested:
            raise JobCancelledError

    def set_row_progress(self, stage: str, processed: int, total: int, progress: int, overall: tuple[int, int]) -> int:
        """Update row based progress to rich progress bar."""
        self.check_cancelled()
        if self.rich_progress is None:
            self.rows_progress = 0
            self.rich_progress = self._progress_bar()

        self.rows_processed = processed
        self.rows_total = total

        progress_percentage = max(self.rows_progress, min(int(progress), 100))
        self.rows_progress = progress_percentage
        self.rich_progress.start()

        if self.task_id is None:
            self.task_id = self.rich_progress.add_task(stage, total=total, completed=0)

        self.rich_progress.update(
            self.task_id,
            completed=processed,
            description=f'{stage} ({progress_percentage}%)',
        )

        if overall is not None:
            start, end = overall
            self.percentage = start + int(progress_percentage / 100 * (end - start))
            self.stage = stage

        return progress_percentage

    def set_progress(self, stage: str) -> None:
        """Set overall progress using predefined stages with fixed percentages."""
        self.check_cancelled()
        progress_stages: dict[str, tuple] = {
            'start': ('start', 0),
            'file_loaded': ('file loaded', 8),
            'init_model': ('initializing model', 10),
            'init_tables': ('initializing tables', 15),
            'init_names': ('initializing name detection', 18),
            'done': ('done', 100),
        }

        self.rows_processed = None
        self.rows_total = None

        current_stage = progress_stages[stage]
        self.percentage = max(0, min(current_stage[1], 100))
        self.stage = current_stage[0]

        logger.debug('Overall progress: %s (%d%%)\n', current_stage[0], self.percentage)

    def mark_done(self, status: str = 'done') -> None:
        """Mark the process as terminal with the given status.

        Valid values: 'done', 'cancelled', 'error'.
        percentage is left at its current value so the gateway sees real progress.
        """
        self.rows_processed = None
        self.rows_total = None
        self.stage = status

    def get_progress(self) -> dict[str, int | str | None]:
        """Retrieve the overall progress percentage and stage description for real-time reporting."""
        return {
            'stage': self.stage,
            'percentage': self.percentage,
            'rows_total': self.rows_total,
            'rows_processed': self.rows_processed,
        }


def performance_metrics(start_time: float, df_rowcount: int) -> dict[str, int | float]:
    """Log performance metrics in time needed for processing."""
    end_time = time.time()
    total_time = end_time - start_time
    time_per_row = (total_time / df_rowcount * 1000) if df_rowcount > 0 else 0
    total_seconds = int(timedelta(seconds=total_time).total_seconds())

    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    logger.info('Time passed with a total of %d rows', df_rowcount)
    logger.info('Total time: %dh %dm %ds (%.3f ms per row)', hours, minutes, seconds, time_per_row)

    return {
        'total_rows': df_rowcount,
        'hours': hours,
        'minutes': minutes,
        'seconds': seconds,
        'time_per_row': round(time_per_row, 3),
    }
