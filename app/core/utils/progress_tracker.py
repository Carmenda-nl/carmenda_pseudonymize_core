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
        self.task_id: TaskID | None = None
        self.rich_progress: Progress | None = None
        self.rows_progress = 0
        self.rows_processed: int | None = None
        self.rows_total: int | None = None
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
            self.overall_progress = start + int(progress_percentage / 100 * (end - start))
            self.overall_stage = stage
        return progress_percentage

    def set_progress(self, stage: str) -> None:
        """Set overall progress using predefined stages with fixed percentages."""
        progress_stages: dict[str, tuple] = {
            'start': ('start', 0),
            'sanitize_csv': ('processing csv', 3),
            'normalize_csv': ('normalizing csv', 6),
            'file_loaded': ('file loaded', 8),
            'init_deduce': ('initializing deduce', 10),
            'init_tables': ('initializing tables', 15),
            'init_names': ('initializing name detection', 18),
            'done': ('done', 100),
        }

        # reset row progress when setting overall stages (mostly on re-runs)
        self.rows_processed = None
        self.rows_total = None

        current_stage = progress_stages[stage]
        self.overall_progress = max(0, min(current_stage[1], 100))
        self.overall_stage = current_stage[0]
        logger.debug('Overall progress: %s (%d%%)\n', current_stage[0], self.overall_progress)

    def get_progress(self) -> dict[str, int | str | None]:
        """Retrieve the overall progress percentage and stage description for websocket reporting."""
        return {
            'stage': self.overall_stage,
            'percentage': self.overall_progress,
            'rows_total': self.rows_total,
            'rows_processed': self.rows_processed,
        }


# Singleton instance (only one instance needed)
tracker = ProgressTracker()


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
