# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Persistent config values model for the Django project."""

from django.core.exceptions import ValidationError
from django.db import models


class ConfigValues(models.Model):
    """Model to store config values for the application."""

    language = models.CharField(max_length=2, default='en')

    def __str__(self) -> str:
        """Return a string representation of the config values."""
        return 'Config values'

    def clean(self) -> None:
        """Ensure only one config values instance exists."""
        super().clean()
        if not self.id and ConfigValues.objects.exists():
            reason = 'Only one instance of config values is allowed.'
            raise ValidationError(reason)
