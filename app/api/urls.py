# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""URL configuration for the API (Django Rest Framework)."""

from django.conf import settings
from django.urls import include, path
from rest_framework.routers import DefaultRouter

from api.views import DeidentificationJobViewSet

router = DefaultRouter()
router.register('v1/jobs', DeidentificationJobViewSet, basename='jobs')

urlpatterns = [
    path('', include(router.urls)),
]

if settings.DEBUG:
    from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView

    urlpatterns += [
        path('docs/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
        path('schema/', SpectacularAPIView.as_view(), name='schema'),
    ]
