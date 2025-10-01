# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Progress tracking utilities for multi-stage data processing.

This module provides the ProgressTracker class for managing and reporting
progress across various processing stages in the core.
"""

from __future__ import annotations

import logging
import sys

from utils.logger import setup_logging, setup_progress_logger

logger = setup_logging()


class ProgressTracker:
    """Track progress across multiple stages of data processing."""

    def __init__(self) -> None:
        """Initialize the ProgressTracker with default values."""
        self.progress = 0
        self.current_stage = None
        self.current_step = None

        # Progress stages configuration
        self.progress_stages = [
            'Loading data',
            'Pre-processing',
            'Pseudonymization',
            'Data transformation',
            'Filtering nulls',
            'Writing output',
            'Finalizing',
        ]
        self.total_stages = len(self.progress_stages)
        self.current_stage_index = 0
        self.last_stage_name = None

    def update(self, stage_name: str | None = None, step_name: str | None = None, step_progress: float = 0) -> int:
        """Update progress to next stage and return current percentage."""
        if stage_name is None and self.current_stage_index < self.total_stages:
            stage_name = self.progress_stages[self.current_stage_index]

        # Only advance stage counter if this is a genuinely new stage
        if step_name is None and stage_name != self.last_stage_name:
            # Find the correct stage index
            try:
                stage_index = self.progress_stages.index(stage_name)
                self.current_stage_index = stage_index + 1  # Set to next stage
            except ValueError:
                self.current_stage_index += 1
            self.last_stage_name = stage_name

        # Calculate percentage based on the stage name (not the counter)
        try:
            stage_index = self.progress_stages.index(stage_name) if stage_name else 0
        except ValueError:
            stage_index = self.current_stage_index

        # Calculate percentage within specified stage
        stage_width = 100 / self.total_stages

        if step_name is None:
            # For stage completion without steps, use the END of the stage
            progress_percentage = min(int(((stage_index + 1) / self.total_stages) * 100), 100)
        else:
            # For steps within a stage, use base + step progress
            base_percentage = int((stage_index / self.total_stages) * 100)

            if step_progress > 0:
                # Use actual step progress to calculate percentage within stage
                within_stage_progress = (step_progress / 100) * stage_width
                progress_percentage = min(base_percentage + int(within_stage_progress), 100)
            else:
                # If we have a step but no specific progress, add small increment
                step_increment = min(stage_width * 0.3, 5)  # 30% of stage or max 5%
                progress_percentage = min(base_percentage + int(step_increment), 100)

        # Update internal state
        self.progress = progress_percentage
        self.current_stage = stage_name
        self.current_step = step_name

        # Only log progress if logger is set to DEBUG and NOT running as a frozen executable
        if logger.level == logging.DEBUG and not getattr(sys, 'frozen', False):
            self._log_progress(progress_percentage, stage_name, step_name)

        return progress_percentage

    def update_with_percentage(self, stage_name: str, step_name: str, step_progress: float) -> int:
        """Update step progress within current stage without going backwards."""
        # Get current progress percentage to avoid going backwards
        current_percentage = self.progress

        # Find the stage index for the given stage name
        try:
            stage_index = self.progress_stages.index(stage_name)
        except ValueError:
            # If stage not found, use current stage
            stage_index = self.current_stage_index

        # Calculate percentage within specified stage
        stage_width = 100 / self.total_stages
        base_percentage = int((stage_index / self.total_stages) * 100)

        # Use actual step progress to calculate percentage within stage
        within_stage_progress = (step_progress / 100) * stage_width
        calculated_percentage = base_percentage + int(within_stage_progress)

        # Never go backwards - use the higher of current or calculated
        progress_percentage = max(current_percentage, calculated_percentage)
        progress_percentage = min(progress_percentage, 100)

        # Update internal state
        self.progress = progress_percentage
        self.current_stage = stage_name
        self.current_step = step_name

        # Only log progress if logger is set to DEBUG and NOT running as a frozen executable
        if logger.level == logging.DEBUG and not getattr(sys, 'frozen', False):
            self._log_progress(progress_percentage, stage_name, step_name)

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

    def _generate_progress_bar(self, percentage: int) -> str:
        """Generate a visual progress bar."""
        width = 30
        filled = int(width * percentage / 100)
        bar = '█' * filled + '░' * (width - filled)
        return f'[{bar}]'

    def _log_progress(self, percentage: int, stage_name: str, step_name: str | None = None) -> None:
        """Log the progress bar to terminal with optional step details."""
        progress_logger = setup_progress_logger()

        green = '\033[92m'
        reset = '\033[0m'
        bar = self._generate_progress_bar(percentage)

        # Combine stage and step into one line
        display_text = f'{stage_name} - {step_name}' if step_name else stage_name

        progress_logger.debug('%s[%3d%%]%s %s %s\n', green, percentage, reset, bar, display_text)


# Singleton instance (only one instance needed)
tracker = ProgressTracker()
