# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""ASGI configuration for the Django project."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, cast

from channels.auth import AuthMiddlewareStack
from channels.routing import ProtocolTypeRouter, URLRouter
from django.core.asgi import get_asgi_application

if TYPE_CHECKING:
    from channels.routing import _ExtendedURLPattern

# Make asyncio and related loggers less noisy
logging.getLogger('asyncio').setLevel(logging.WARNING)
logging.getLogger('autobahn').setLevel(logging.WARNING)
logging.getLogger('daphne').setLevel(logging.WARNING)
logging.getLogger('daphne.http_protocol').setLevel(logging.WARNING)
logging.getLogger('daphne.ws_protocol').setLevel(logging.WARNING)


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'main.settings')

django_asgi = get_asgi_application()

# Import routing after Django is initialized
from api.websocket import routing  # noqa: E402

application: ProtocolTypeRouter = ProtocolTypeRouter(
    {
        'http': django_asgi,
        'websocket': AuthMiddlewareStack(
            URLRouter(cast('list[_ExtendedURLPattern | URLRouter]', routing.websocket_urlpatterns)),
        ),
    },
)
