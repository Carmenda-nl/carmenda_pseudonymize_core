# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Custom storage functionality for the API."""

from django.core.files.storage import FileSystemStorage


class OverwriteStorage(FileSystemStorage):
    """FileSystemStorage that overwrites existing files."""

    def get_available_name(self, name: str, max_length: int | None = None) -> str:
        """Return the given filename, deleting any existing file first."""
        if self.exists(name):
            self.delete(name)
        return name
