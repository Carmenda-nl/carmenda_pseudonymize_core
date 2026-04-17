# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Middleware to serve media files in a Django application."""

from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import TYPE_CHECKING

from django.conf import settings
from django.http import FileResponse, HttpResponseNotFound
from django.utils.translation import gettext as _

if TYPE_CHECKING:
    from collections.abc import Callable

    from django.http import FileResponse as FileResponseType
    from django.http import HttpRequest, HttpResponse

    _HttpResponse = HttpResponse | FileResponseType


class ServeMediaFilesMiddleware:
    """Specifically designed for use with PyInstaller-bundled applications."""

    def __init__(self, get_response: Callable[[HttpRequest], _HttpResponse]) -> None:
        """Initialize the ServeMediaFilesMiddleware."""
        self.get_response = get_response

        # Check if mime types are correct
        mimetypes.init()

        # Assures if the `MEDIA_URL` ends with a slash
        if not settings.MEDIA_URL.endswith('/'):
            settings.MEDIA_URL = f'{settings.MEDIA_URL}/'

    def __call__(self, request: HttpRequest) -> _HttpResponse:
        """Handle incoming requests and serve media files."""
        if request.path.startswith(settings.MEDIA_URL):
            relative_path = request.path[len(settings.MEDIA_URL) :]

            # Build a absolute file path
            file_path = Path(settings.MEDIA_ROOT) / relative_path

            # Checks if the file exists and is readable
            if file_path.exists() and file_path.is_file():
                try:
                    file_obj = file_path.open('rb')
                    content_type, _encoding = mimetypes.guess_type(str(file_path))

                    if content_type is None:
                        content_type = 'application/octet-stream'

                    # Sends the file as response
                    response = FileResponse(file_obj, content_type=content_type)

                    if 'download' in request.GET:
                        response['Content-Disposition'] = f'attachment; filename="{file_path.name}"'

                except (OSError, PermissionError):
                    # File can not be opened
                    return HttpResponseNotFound(_('Can not open the file: {path}').format(path=relative_path))
                else:
                    return response

            return HttpResponseNotFound(_('File {path} not found.').format(path=relative_path))

        return self.get_response(request)
