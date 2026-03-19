# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Application version information."""

import sys
from pathlib import Path

import git

FILE_DIR = Path(__file__).parent

VERSION_FILE = (
    Path(sys._MEIPASS) / 'app' / 'version.txt' if hasattr(sys, '_MEIPASS') else FILE_DIR.parent / 'version.txt'
)


def get_version() -> str:
    """When frozen git is unavailable, so create version.txt as fallback."""
    if not hasattr(sys, '_MEIPASS'):
        repo = git.Repo(FILE_DIR, search_parent_directories=True)
        return repo.git.describe('--tags', '--abbrev=0')

    try:
        return VERSION_FILE.read_text(encoding='utf-8').strip()
    except FileNotFoundError:
        return 'unknown'


def write_version(target: Path | None = None) -> str:
    """Write the current version to a file."""
    version = get_version()
    path = target or VERSION_FILE
    path.write_text(version, encoding='utf-8')

    return version
