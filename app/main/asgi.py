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
from channels.security.websocket import AllowedHostsOriginValidator
from django.core.asgi import get_asgi_application

from api.websocket import routing

if TYPE_CHECKING:
    from channels.routing import _ExtendedURLPattern

# Make asyncio and related loggers less noisy in the logs
logging.getLogger('asyncio').setLevel(logging.WARNING)
logging.getLogger('autobahn').setLevel(logging.WARNING)
logging.getLogger('daphne').setLevel(logging.WARNING)
logging.getLogger('daphne.http_protocol').setLevel(logging.WARNING)
logging.getLogger('daphne.ws_protocol').setLevel(logging.WARNING)


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'main.settings')

application: ProtocolTypeRouter = ProtocolTypeRouter(
    {
        'http': get_asgi_application(),
        'websocket': AllowedHostsOriginValidator(
            AuthMiddlewareStack(
                URLRouter(cast('list[_ExtendedURLPattern | URLRouter]', routing.websocket_urlpatterns)),
            )
        ),
    },
)
