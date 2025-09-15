# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API models for keeping track of the deidentification process."""

import uuid

from django.db import models


class DeidentificationJob(models.Model):
    """Maintaining state information and file references."""

    STATUS_CHOICES = (
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    )

    job_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    input_cols = models.CharField(max_length=500, null=False, blank=False)
    input_file = models.FileField(upload_to='input')
    output_file = models.FileField(upload_to='output', null=True, blank=True)
    key_file = models.FileField(upload_to='input', null=True, blank=True)
    log_file = models.FileField(upload_to='output', null=True, blank=True)
    zip_file = models.FileField(upload_to='output', null=True, blank=True)
    zip_preview = models.JSONField(null=True, blank=True)
    processed_preview = models.JSONField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    error_message = models.TextField(default='', blank=True)

    def __str__(self) -> str:
        """Return a string representation of the deidentification job."""
        return f'Job {self.job_id} - {self.status}'
