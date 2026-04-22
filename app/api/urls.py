# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""URL configuration for the API (Django Rest Framework)."""

from __future__ import annotations

from django.conf import settings
from django.urls import include, path
from rest_framework.routers import DefaultRouter

from api.views import APIRootView, DeidentificationJobViewSet, VersionView
from api.views.root_views import ConfigValuesView
from api.views.sse_views import job_progress

router = DefaultRouter()
router.register('v1/jobs', DeidentificationJobViewSet, basename='jobs')

urlpatterns = [
    path('', APIRootView.as_view(), name='api-root'),
    path('', include(router.urls)),
    path('v1/version/', VersionView.as_view(), name='version'),
    path('v2/settings/', ConfigValuesView.as_view(), name='settings'),
    path('v2/jobs/<str:job_id>/progress/', job_progress, name='job-progress'),
]

if settings.DEBUG:
    from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView

    urlpatterns += [
        path('v1/docs/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
        path('v1/schema/', SpectacularAPIView.as_view(), name='schema'),
    ]
