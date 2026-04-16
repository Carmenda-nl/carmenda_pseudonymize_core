# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API service layer for deidentification job processing.

This package runs deidentification jobs using two threads:
    - Thread 1 (job_runner):
      executes the core processing in the background so the
      HTTP request can return immediately with a 202 Accepted response.

    - Thread 2 (job_monitor):
      polls the progress tracker every 100ms while Thread 1
      is blocked on processing, and forwards updates to the frontend via WebSocket.
"""
