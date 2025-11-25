# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API models for keeping track of the deidentification process."""

import uuid
from pathlib import Path

from django.db import models
from django.utils.text import get_valid_filename


def filepath(instance: 'DeidentificationJob', filename: str) -> str:
    """Keep filename, but store it under a UUID folder."""
    safe_name = get_valid_filename(Path(filename).name)
    job_id = getattr(instance, 'job_id', None)
    job_part = str(job_id) if job_id is not None else ''
    return str(Path(job_part) / safe_name)


def input_path(instance: 'DeidentificationJob', filename: str) -> str:
    """Generate the input file path."""
    base_path = filepath(instance, filename)
    return str(Path('input') / base_path)


def output_path(instance: 'DeidentificationJob', filename: str) -> str:
    """Generate the output file path."""
    base_path = filepath(instance, filename)
    return str(Path('output') / base_path)


class DeidentificationJob(models.Model):
    """Maintaining state information and file references."""

    STATUS_CHOICES = (
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('cancelled', 'Cancelled'),
        ('failed', 'Failed'),
    )

    job_id = models.UUIDField(default=uuid.uuid1, editable=False, primary_key=True)
    input_cols = models.CharField(blank=True)
    input_file = models.FileField(upload_to=input_path)
    datakey = models.FileField(upload_to=input_path, null=True, blank=True)
    output_file = models.FileField(upload_to=output_path, null=True, blank=True)
    log_file = models.FileField(upload_to=output_path, null=True, blank=True)
    error_rows_file = models.FileField(upload_to=output_path, null=True, blank=True)
    zip_file = models.FileField(upload_to=output_path, null=True, blank=True)
    zip_preview = models.JSONField(null=True, blank=True)
    preview = models.JSONField(null=True, blank=True)
    processed_preview = models.JSONField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    error_message = models.TextField(default='', blank=True)

    def __str__(self) -> str:
        """Return a string representation of the deidentification job."""
        return f'Job {self.job_id} - {self.status}'
