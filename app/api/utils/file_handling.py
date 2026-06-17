# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Utilities for the API."""

from __future__ import annotations

import contextlib
from pathlib import Path

from main.config import settings


def cleanup_output() -> None:
    """Remove all files from the output folder."""
    output_root = Path(settings.output_folder)

    with contextlib.suppress(OSError):
        for artifact in output_root.iterdir():
            if artifact.is_file():
                artifact.unlink(missing_ok=True)
