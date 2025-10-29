# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Progress tracking utilities for data processing.

This module provides the ProgressTracker for managing and reporting
progress during data transformation using Rich library.
"""

from __future__ import annotations

import sys
import time

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
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
        """Initialize the ProgressTracker."""
        self._default_state()

    def _default_state(self) -> None:
        """Set all state variables to default values."""
        self.current_stage = None
        self.current_step = None
        self.start_time = None
        self.total_rows = 0
        self.rows_processed = 0
        self.progress = 0
        self.task_id = None
        self.rich_progress = self._progress_bar()

    def _progress_bar(self) -> Progress:
        """Create a Rich progress bar with spinner."""
        spinner = SpinnerColumn()
        text = TextColumn('[bold blue]{task.description}', justify='left')
        bar = BarColumn(bar_width=40)
        task_progress = TaskProgressColumn()
        mofn = MofNCompleteColumn()
        time_elapsed = TimeElapsedColumn()
        time_remaining = TimeRemainingColumn()

        return Progress(spinner, text, bar, task_progress, mofn, time_elapsed, time_remaining)

    def _parse_row_progress(self, step_name: str) -> None:
        """Parse row progress from step_name format: 'Processed X/Y rows'."""
        if not step_name or '/' not in step_name:
            return

        try:
            parts = step_name.split()
            min_parts_required = 2

            if len(parts) < min_parts_required:
                return

            # Find the part containing the fraction (X/Y)
            fraction_part = next((part for part in parts if '/' in part), None)
            if not fraction_part:
                return

            current_str, total_str = fraction_part.split('/', 1)
            current_rows = int(current_str.replace(',', ''))
            total_rows = int(total_str.replace(',', ''))

            self.total_rows = total_rows
            self.rows_processed = current_rows

        except (ValueError, IndexError) as error:
            logger.debug("Failed to parse row progress from '%s': %s", step_name, error)

    def update_progress(self, stage_name: str, step_name: str, step_progress: int) -> int:
        """Update step and row progress to rich progress bar."""
        self._parse_row_progress(step_name)

        # Calculate percentage (use step_progress as overall progress)
        progress_percentage = max(self.progress, min(int(step_progress), 100))

        self.progress = progress_percentage
        self.current_stage = stage_name
        self.current_step = step_name

        self.rich_progress.start()
        self.start_time = time.time()

        if self.task_id is None:
            self.task_id = self.rich_progress.add_task(f'{stage_name}', total=self.total_rows, completed=0)

        self.rich_progress.update(
            self.task_id,
            completed=self.rows_processed,
            description=f'{stage_name} ({progress_percentage}%)',
        )
        return progress_percentage

    def get_progress(self) -> dict[str, int | str | None]:
        """Retrieve the current progress and stage information."""
        combined_stage = None

        if self.current_stage and self.current_step:
            combined_stage = f'{self.current_stage} - {self.current_step}'
        elif self.current_stage:
            combined_stage = self.current_stage

        return {'percentage': self.progress, 'stage': combined_stage, 'step': self.current_step}

    def finalize_progress(self, complete: str = 'yes') -> None:
        """Finalize progress and stop the Rich progress bar."""
        complete_flag = complete.strip().lower() in ('yes', 'true', '1')

        if complete_flag and self.task_id is not None:
            final_value = self.total_rows if self.total_rows > 0 else 100
            self.rich_progress.update(self.task_id, completed=final_value)
            self.progress = 100

        if self.rich_progress is not None:
            self.rich_progress.stop()
            sys.stdout.write('\n')  # <-- whitespace under progressbar

        self.rich_progress = None
        self.task_id = None

    def reset(self) -> None:
        """Reset the tracker for a new processing run."""
        if self.rich_progress is not None:
            self.rich_progress.stop()
        self._default_state()


# Singleton instance (only one instance needed)
tracker = ProgressTracker()


def performance_metrics(start_time: float, df_rowcount: int) -> None:
    """Log performance metrics in time needed for processing."""
    end_time = time.time()
    total_time = end_time - start_time
    time_per_row = total_time / df_rowcount if df_rowcount > 0 else 0
    minute = 60

    if total_time >= minute:
        minutes = int(total_time // minute)
        seconds = total_time % minute
        time_str = f'{minutes} minutes and {seconds:.2f} seconds'
    else:
        time_str = f'{total_time:.2f} seconds'

    logger.info('Time passed with a total of %d rows', df_rowcount)
    logger.info('Total time: %s (%.6f seconds per row)', time_str, time_per_row)
