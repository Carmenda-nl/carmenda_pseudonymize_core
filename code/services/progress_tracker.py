# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

from threading import Lock


class ProgressTracker:
    def __init__(self):
        self.progress = 0
        self.current_stage = None
        self.lock = Lock()

    def update_progress(self, percentage, stage=None):
        """
        Update the progress tracker with new percentage and stage information.

        Parameters:
            percentage (int): Progress percentage (0-100)
            stage (str, optional): Current processing stage name
        """
        self.progress = percentage

        if stage is not None:
            self.current_stage = stage

    def get_progress(self):
        """
        Returns the current status as a dictionary.
        """
        return {
            'percentage': self.progress,
            'stage': self.current_stage
        }


# singleton instance
tracker = ProgressTracker()
