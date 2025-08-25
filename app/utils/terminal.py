# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Terminal utility functions."""

import os


def get_terminal_width(fallback: int = 80) -> int:
    """Get terminal width with fallback."""
    try:
        return os.get_terminal_size().columns
    except (AttributeError, OSError):
        return fallback


def get_separator_line(char: str = '-', padding: int = 0) -> str:
    """Get a separator line that fits the terminal width."""
    width = get_terminal_width() - padding
    return char * width
