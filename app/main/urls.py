# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""URL configuration for the Django project."""

from django.conf import settings
from django.conf.urls.static import static
from django.urls import include, path
from django.views.generic import RedirectView

from api.views import favicon_view

urlpatterns = [
    path('', RedirectView.as_view(url='/api/', permanent=True)),
    path('api/', include('api.urls')),
    path('favicon.ico', favicon_view),
    *static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT),
]
