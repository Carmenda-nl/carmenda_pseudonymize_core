# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API service layer for deidentification job processing.

This package runs deidentification jobs in a background thread (job_runner)
so the HTTP request can return immediately. Progress is tracked via the
progress tracker and streamed to the frontend using Server-Sent Events (SSE).
"""
