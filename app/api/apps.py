# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API app for the Django project."""

from django.apps import AppConfig


class ApiConfig(AppConfig):
    """Base configuration for the current app."""

    default_auto_field = 'django.db.models.BigAutoField'
    name = 'api'
