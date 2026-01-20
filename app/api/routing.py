# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""WebSocket routing for real-time job progress updates."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from django.urls import URLPattern, path

from api import consumers

if TYPE_CHECKING:
    from collections.abc import Callable

    from django.http import HttpResponseBase

websocket_urlpatterns: list[URLPattern] = [
    path(
        'ws/jobs/<str:job_id>/progress/',
        cast('Callable[..., HttpResponseBase]', consumers.JobProgressConsumer.as_asgi()),
    ),
]
