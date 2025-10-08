# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Progress tracking utilities for data processing.

This module provides the ProgressTracker class for managing and reporting
progress across various processing stages in the core.
"""

from __future__ import annotations

import logging
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
    """Track progress across multiple stages of data processing using Rich."""

    def __init__(self) -> None:
        """Initialize the ProgressTracker with default values."""
        self.progress = 0
        self.current_stage = None
        self.current_step = None
        self.start_time = None
        self.rows_processed = 0
        self.total_rows = 0

        # Progress stages configuration
        self.progress_stages = [
            'Pre-processing',
            'Data transformation',
            'Finalizing',
        ]
        self.total_stages = len(self.progress_stages)
        self.current_stage_index = 0
        self.last_stage_name = None

        # Rich progress instance
        self.rich_progress = None
        self.task_id = None

    def _create_progress_bar(self) -> Progress:
        """Create a Rich progress bar with custom columns."""
        return Progress(
            SpinnerColumn(),
            TextColumn('[bold blue]{task.description}', justify='left'),
            BarColumn(bar_width=40),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            transient=False,
            refresh_per_second=10,
        )

    def update(self, stage_name: str, step_name: str = '', step_progress: float = 0.0) -> None:
        """Update progress for a specific stage and step."""
        # Extract row count from step_name if available (format: "Processed X/Y rows")
        if step_name and '/' in step_name:
            try:
                parts = step_name.split()
                if len(parts) >= 2 and '/' in parts[1]:
                    current_rows_str = parts[1].split('/')[0].replace(',', '')
                    total_rows_str = parts[1].split('/')[1].replace(',', '')
                    self.rows_processed = int(current_rows_str)
                    self.total_rows = int(total_rows_str)
            except (ValueError, IndexError):
                pass

        # Calculate percentage
        current_percentage = self.progress
        try:
            stage_index = self.progress_stages.index(stage_name)
        except ValueError:
            stage_index = self.current_stage_index

        stage_width = 100 / self.total_stages
        base_percentage = int((stage_index / self.total_stages) * 100)
        within_stage_progress = (step_progress / 100) * stage_width
        calculated_percentage = base_percentage + int(within_stage_progress)

        progress_percentage = max(current_percentage, calculated_percentage)
        progress_percentage = min(progress_percentage, 100)

        self.progress = progress_percentage
        self.current_stage = stage_name
        self.current_step = step_name

        # Only show Rich progress in DEBUG mode
        if logger.level == logging.DEBUG and not getattr(sys, 'frozen', False):
            if self.rich_progress is None:
                self.rich_progress = self._create_progress_bar()
                self.rich_progress.start()
                self.start_time = time.time()

            # Only create task when we have row tracking data
            # Skip pre-processing phase entirely
            if self.total_rows > 0 and self.task_id is None:
                self.task_id = self.rich_progress.add_task(
                    f'{stage_name}',
                    total=self.total_rows,
                    completed=self.rows_processed,
                )

            # Update task only if it exists (row tracking active)
            if self.task_id is not None:
                self.rich_progress.update(
                    self.task_id,
                    completed=self.rows_processed,
                    description=f'{stage_name}',
                )

    def update_with_percentage(self, stage_name: str, step_name: str, step_progress: float) -> int:
        """Update step progress within current stage without going backwards."""
        # Extract row count from step_name if available (format: "Processed X/Y rows")
        if step_name and '/' in step_name:
            try:
                parts = step_name.split()
                if len(parts) >= 2 and '/' in parts[1]:
                    current_rows_str = parts[1].split('/')[0].replace(',', '')
                    total_rows_str = parts[1].split('/')[1].replace(',', '')
                    new_rows = int(current_rows_str)
                    new_total = int(total_rows_str)

                    # Update row tracking
                    if new_total > 0:
                        self.total_rows = new_total
                        self.rows_processed = new_rows
            except (ValueError, IndexError):
                pass

        # Calculate percentage
        current_percentage = self.progress
        try:
            stage_index = self.progress_stages.index(stage_name)
        except ValueError:
            stage_index = self.current_stage_index

        stage_width = 100 / self.total_stages
        base_percentage = int((stage_index / self.total_stages) * 100)
        within_stage_progress = (step_progress / 100) * stage_width
        calculated_percentage = base_percentage + int(within_stage_progress)

        progress_percentage = max(current_percentage, calculated_percentage)
        progress_percentage = min(progress_percentage, 100)

        self.progress = progress_percentage
        self.current_stage = stage_name
        self.current_step = step_name

        # Only show Rich progress in DEBUG mode
        if logger.level == logging.DEBUG and not getattr(sys, 'frozen', False):
            if self.rich_progress is None:
                self.rich_progress = self._create_progress_bar()
                self.rich_progress.start()
                self.start_time = time.time()

            # Only create task when we have row tracking data
            if self.total_rows > 0 and self.task_id is None:
                self.task_id = self.rich_progress.add_task(
                    f'{stage_name}',
                    total=self.total_rows,
                    completed=self.rows_processed,
                )

            # Update task only if it exists (row tracking active)
            if self.task_id is not None:
                self.rich_progress.update(
                    self.task_id,
                    completed=self.rows_processed,
                    description=f'{stage_name}',
                )

        return progress_percentage

    def get_progress(self) -> dict[str, int | str | None]:
        """Retrieve the current progress and stage information."""
        # Combine stage and step for display
        combined_stage = None
        if self.current_stage and self.current_step:
            combined_stage = f'{self.current_stage} - {self.current_step}'
        elif self.current_stage:
            combined_stage = self.current_stage

        return {
            'percentage': self.progress,
            'stage': combined_stage,
            'step': self.current_step,
        }

    def finalize_progress(self) -> None:
        """Finalize progress and stop the Rich progress bar."""
        if self.rich_progress is not None:
            if self.task_id is not None:
                final_value = self.total_rows if self.total_rows > 0 else 100
                self.rich_progress.update(self.task_id, completed=final_value)
            self.rich_progress.stop()
            # Print newline to separate progress bar from next output
            print()
            self.rich_progress = None
            self.task_id = None


# Singleton instance (only one instance needed)
tracker = ProgressTracker()


def performance_metrics(start_time: float, df_rowcount: int) -> None:
    """Log performance metrics in time needed for processing."""
    end_time = time.time()
    total_time = end_time - start_time
    time_per_row = total_time / df_rowcount if df_rowcount > 0 else 0

    logger.info('Total time: %.2f seconds (%.6f seconds per row)', total_time, time_per_row)
