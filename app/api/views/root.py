# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API root view with documentation links."""

from __future__ import annotations

from typing import TYPE_CHECKING

from django.conf import settings
from drf_spectacular.utils import extend_schema
from rest_framework.response import Response
from rest_framework.reverse import reverse
from rest_framework.views import APIView

from api.schemas import API_ROOT_SCHEMA

if TYPE_CHECKING:
    from rest_framework.request import Request


class ApiTags:
    """API tag constants for Swagger/OpenAPI documentation grouping.

    These tags are used to categorize endpoints in the API documentation
    for better organization and discoverability.
    """

    API = 'API'
    JOBS = 'Jobs'
    PROCESSING = 'Processing'
    CANCEL = 'Cancel'


@extend_schema(tags=[ApiTags.API])
@API_ROOT_SCHEMA
class APIRootView(APIView):
    """Custom API root view that includes documentation links."""

    def get(self, request: Request, format_suffix: str | None = None) -> Response:
        """Return links to all available endpoints."""
        data = {
            'v1/jobs': reverse('jobs-list', request=request, format=format_suffix),
        }
        if settings.DEBUG:
            data['v1/docs'] = reverse('swagger-ui', request=request, format=format_suffix)
            data['v1/schema'] = reverse('schema', request=request, format=format_suffix)

        return Response(data)
