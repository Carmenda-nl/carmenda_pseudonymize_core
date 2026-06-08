# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Application version information."""

from pathlib import Path

from main._version import __version__

FILE_DIR = Path(__file__).parent


def get_version() -> str:
    """Resolve version from git tag or _version.py."""
    try:
        import git
    except ImportError:
        return __version__

    try:
        repo = git.Repo(FILE_DIR, search_parent_directories=True)
        return repo.git.describe('--tags', '--abbrev=0')
    except (git.GitCommandError, git.InvalidGitRepositoryError):
        return __version__
