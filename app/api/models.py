# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API models for keeping track of the deidentification process."""

from pathlib import Path

from django.db import models


class DeidentificationJob(models.Model):
    """Maintaining state information and file references."""

    STATUS_CHOICES = (
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    )

    job_id = models.CharField(max_length=250, primary_key=True, editable=False)
    input_cols = models.CharField(null=False, blank=False)
    input_file = models.FileField(upload_to='input')
    key_file = models.FileField(upload_to='input', null=True, blank=True)
    output_file = models.FileField(upload_to='output', null=True, blank=True)
    log_file = models.FileField(upload_to='output', null=True, blank=True)
    zip_file = models.FileField(upload_to='output', null=True, blank=True)
    zip_preview = models.JSONField(null=True, blank=True)
    processed_preview = models.JSONField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    error_message = models.TextField(default='', blank=True)

    def __str__(self) -> str:
        """Return a string representation of the deidentification job."""
        return f'Job {self.job_id} - {self.status}'

    def save(self, *args: object, **kwargs: object) -> None:
        """Override save to automatically set job_id from input filename."""
        if not self.job_id and self.input_file:
            filename = Path(self.input_file.name).stem
            base_id = filename.replace(' ', '_')
            job_id = base_id

            # Check if name already exists, if so, append a counter
            counter = 1

            while DeidentificationJob.objects.filter(job_id=job_id).exists():
                job_id = f'{base_id}_{counter}'
                counter += 1

            self.job_id = job_id

        super().save(*args, **kwargs)
