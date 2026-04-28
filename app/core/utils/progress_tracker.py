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

try:
    from django.utils.translation import gettext_lazy as _
except ImportError:
    def _(s: str) -> str:  # type: ignore[misc]
        return s

from .logger import setup_logging

logger = setup_logging()


class ProgressTracker:
    """Track progress for data transformation using Rich."""

    def __init__(self) -> None:
        """Initialize the progress tracker."""
        self._default_state()

    def _default_state(self) -> None:
        """Set/reset the default progress tracking state values.

        Collects row progress & stage for the terminal pseudonymization progress bar
        and overall progress & stage for the WebSocket updates to the frontend.
        """
        self.rich_progress: Progress | None = None
        self.task_id: TaskID | None = None

        self.progress = 0
        self.row_description: str | None = None

        self.overall_progress: int = 0
        self.overall_stage: str | None = None

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

    def set_row_progress(self, stage: str, processed: int, total: int, progress: int, overall: tuple[int, int]) -> int:
        """Update row based progress to rich progress bar."""
        if self.rich_progress is None:
            self.progress = 0
            self.rich_progress = self._progress_bar()

        self.row_description = f'Processed {processed}/{total} rows'

        # Calculate percentage (use progress as overall progress)
        progress_percentage = max(self.progress, min(int(progress), 100))

        self.progress = progress_percentage

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
            self.overall_progress = start + int(progress_percentage / 100 * (end - start))
            self.overall_stage = stage
        return progress_percentage

    def set_progress(self, stage: str) -> None:
        """Set overall progress using predefined stages with fixed percentages."""
        progress_stages: dict[str, tuple] = {
            'start': (_('Start'), 0),
            'sanitize_csv': (_('Processing CSV'), 3),
            'normalize_csv': (_('Normalizing CSV'), 6),
            'file_loaded': (_('File Loaded'), 8),
            'init_deduce': (_('Initializing Deduce'), 10),
            'init_tables': (_('Initializing Tables'), 15),
            'init_names': (_('Initializing name detection'), 18),
            'done': (_('Done'), 100),
        }

        current_stage = progress_stages[stage]
        self.overall_progress = max(0, min(current_stage[1], 100))
        self.overall_stage = current_stage[0]
        logger.debug('Overall progress: %s (%d%%)', current_stage[0], self.overall_progress)

    def get_progress(self) -> dict[str, int | str | None]:
        """Retrieve the overall progress percentage and stage description for websocket reporting."""
        stage = self.overall_stage
        if stage and self.row_description:
            stage = f'{stage} - {self.row_description}'
        return {'percentage': self.overall_progress, 'stage': stage}


# Singleton instance (only one instance needed)
tracker = ProgressTracker()


def _time_plural(value: int, unit: str) -> str:
    """Helper function to format time units with correct pluralization."""
    suffix = '' if value == 1 else 's'
    return f'{value} {unit}{suffix}'


def performance_metrics(start_time: float, df_rowcount: int) -> dict[str, float]:
    """Log performance metrics in time needed for processing."""
    end_time = time.time()
    total_time = end_time - start_time
    time_per_row = total_time / df_rowcount if df_rowcount > 0 else 0
    elapsed = timedelta(seconds=total_time)

    if elapsed >= timedelta(hours=1):
        total_seconds = int(elapsed.total_seconds())
        hours = total_seconds // 3600
        remainder = total_seconds % 3600
        minutes = remainder // 60
        seconds = remainder % 60
        time_str = (
            f'{_time_plural(hours, "hour")}, {_time_plural(minutes, "minute")} and {_time_plural(seconds, "second")}'
        )
    elif elapsed >= timedelta(minutes=1):
        total_seconds = int(elapsed.total_seconds())
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        time_str = f'{_time_plural(minutes, "minute")} and {_time_plural(seconds, "second")}'
    else:
        time_str = f'{_time_plural(int(elapsed.total_seconds()), "second")}'

    logger.info('Time passed with a total of %d rows', df_rowcount)
    logger.info('Total time: %s (%.6f seconds per row)', time_str, time_per_row)

    return {'total_rows': df_rowcount, 'total_time': total_time, 'time_per_row': time_per_row}
