# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Utilities for the API."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def cleanup_output(output_path: Path) -> None:
    """Remove all files from the given output folder."""
    for artifact in output_path.iterdir():
        if artifact.is_file():
            artifact.unlink(missing_ok=True)
