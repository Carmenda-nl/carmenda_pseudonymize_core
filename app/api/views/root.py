# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API root view with documentation links."""

from __future__ import annotations

from typing import TYPE_CHECKING

from django.conf import settings
from drf_spectacular.utils import extend_schema
from rest_framework import generics
from rest_framework.response import Response
from rest_framework.reverse import reverse
from rest_framework.views import APIView

from api.schemas import API_ROOT_SCHEMA, VERSION_SCHEMA
from api.serializers import ConfigValuesSerializer
from main.version import get_version
from settings.models import ConfigValues

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
            'v1/version': reverse('version', request=request, format=format_suffix),
            'v2/settings': reverse('settings', request=request, format=format_suffix),
        }
        if settings.DEBUG:
            data['v1/docs'] = reverse('swagger-ui', request=request, format=format_suffix)
            data['v1/schema'] = reverse('schema', request=request, format=format_suffix)

        return Response(data)


@extend_schema(tags=[ApiTags.API])
@VERSION_SCHEMA
class VersionView(APIView):
    """Returns the current application version."""

    def get(self, request: Request) -> Response:
        """Return the application version."""
        return Response({'version': get_version()})


@extend_schema(tags=[ApiTags.API], request=ConfigValuesSerializer, responses=ConfigValuesSerializer)
class ConfigValuesView(generics.RetrieveUpdateAPIView):
    """Retrieve and update the application config values."""

    serializer_class = ConfigValuesSerializer
    http_method_names = ('get', 'put', 'patch', 'head', 'options')

    def get_object(self) -> ConfigValues:
        """Return the config values instance, creating it if missing."""
        config_values = ConfigValues.objects.first()

        if config_values is None:
            config_values = ConfigValues.objects.create()

        return config_values
