# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Application version information."""

import sys
from pathlib import Path

import git

FILE_DIR = Path(__file__).parent
VERSION_FILE = FILE_DIR.parent / 'version.txt'


def get_version() -> str:
    """When frozen git is unavailable, so create version.txt as fallback."""
    if not hasattr(sys, 'frozen'):
        repo = git.Repo(FILE_DIR, search_parent_directories=True)
        return repo.git.describe('--tags', '--abbrev=0')

    try:
        return VERSION_FILE.read_text(encoding='utf-8').strip()
    except FileNotFoundError:
        return 'unknown'
