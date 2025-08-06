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




def progress_tracker(tracker):
    """
    Creates a progress tracking mechanism for multi-stage processing.

    Parameters:
        tracker: Global tracker object to update overall progress

    Returns:
        Dictionary containing progress tracking functions and data
    """
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

    def update_progress(stage_name=None):
        if stage_name is None and progress_data['current_stage'] < total_stages:
            stage_name = progress_stages[progress_data['current_stage']]

        progress_data['current_stage'] += 1
        progress_data['progress_percentage'] = int((progress_data['current_stage'] / total_stages) * 100)

        # update the global progress tracker
        tracker.update_progress(progress_data['progress_percentage'], stage_name)

        return progress_data['progress_percentage']

    return {
        'update': update_progress,
        'data': progress_data,
        'get_stage_name': lambda idx: progress_stages[idx] if 0 <= idx < len(progress_stages) else None,
    }