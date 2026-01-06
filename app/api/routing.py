# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""WebSocket routing for real-time job progress updates."""

from django.urls import re_path

from api import consumers

websocket_urlpatterns = [
    re_path(r'ws/jobs/(?P<job_id>[^/]+)/progress/$', consumers.JobProgressConsumer.as_asgi()),
]
