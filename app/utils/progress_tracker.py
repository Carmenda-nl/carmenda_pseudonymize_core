# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Progress tracking utilities for multi-stage data processing.

This module provides the ProgressTracker class and a factory function for managing and reporting
progress across various processing stages in the core.
"""

from __future__ import annotations

import logging
from threading import Lock

from utils.logger import setup_logging

logger = setup_logging()


class ProgressTracker:
    """Singleton class to track progress across multiple stages of data processing."""

    def __init__(self) -> None:
        """Initialize the ProgressTracker with default values."""
        self.progress = 0
        self.current_stage = None
        self.lock = Lock()

    def update_progress(self, percentage: float, stage: str | None = None) -> None:
        """Update the progress tracker with new percentage and stage information."""
        with self.lock:
            self._update_internal_state(percentage, stage)

    def _update_internal_state(self, percentage: float, stage: str | None) -> None:
        """Update the progress and stage safely."""
        self.progress = int(percentage)
        if stage is not None:
            self.current_stage = stage

    def get_progress(self) -> dict[str, int | str | None]:
        """Retrieve the current progress and stage information."""
        with self.lock:
            return {'percentage': self.progress, 'stage': self.current_stage}


# Singleton instance (only one instance needed)
tracker = ProgressTracker()


def progress_tracker(tracker: ProgressTracker) -> dict:
    """Create a progress tracker for multi-stage processing."""
    progress_stages = [
        'Prepare data',
        'Loading data',
        'Pre-processing',
        'Pseudonymization',
        'Data transformation',
        'Filtering nulls',
        'Writing output',
        'Finalizing',
    ]

    total_stages = len(progress_stages)

    progress_data = {
        'current_stage': 0,
        'progress_percentage': 0,
        'total_stages': total_stages,
        'stages': progress_stages,
    }

    def _calculate_percentage() -> int:
        """Calculate current progress percentage."""
        return int((progress_data['current_stage'] / total_stages) * 100)

    def _generate_progress_bar(percentage: int) -> str:
        """Generate a visual progress bar."""
        width = 30

        filled = int(width * percentage / 100)
        bar = '█' * filled + '░' * (width - filled)
        return f'[{bar}]'

    def _log_progress(percentage: int, stage_name: str) -> None:
        """Log the progress with colored output."""
        green = '\033[92m'
        reset = '\033[0m'
        bar = _generate_progress_bar(percentage)

        logger.debug('%s[%3d%%]%s %s %s', green, percentage, reset, bar, stage_name)

    def update_progress(stage_name: str | None = None) -> int:
        """Update progress to next stage and return current percentage."""
        if stage_name is None and progress_data['current_stage'] < total_stages:
            stage_name = progress_stages[progress_data['current_stage']]

        progress_data['current_stage'] += 1
        progress_percentage = _calculate_percentage()
        progress_data['progress_percentage'] = progress_percentage

        # Update the global progress tracker
        tracker.update_progress(progress_percentage, stage_name)

        # Only log progress if logger is set to DEBUG level
        if logger.level == logging.DEBUG:
            _log_progress(progress_percentage, stage_name)

        return progress_percentage

    return {
        'update': update_progress,
        'data': progress_data,
        'get_stage_name': lambda idx: progress_stages[idx] if 0 <= idx < len(progress_stages) else None,
    }
