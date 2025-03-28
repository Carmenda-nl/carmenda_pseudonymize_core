from django.db import models
from django.conf import settings
import uuid
import os


class DeidentificationJob(models.Model):
    """
    Model for tracking the processing of
    healthcare data files through a deidentification pipeline,
    maintaining state information and file references.
    """
    STATUS_CHOICES = (
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    )

    job_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    input_file = models.FileField(upload_to='input')
    output_file = models.FileField(upload_to='output', null=True, blank=True)
    key_file = models.FileField(upload_to='output', null=True, blank=True)
    log_file = models.FileField(upload_to='output', null=True, blank=True)
    processed_preview = models.JSONField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    error_message = models.TextField(blank=True, null=True)

    def __str__(self):
        return f'Job {self.job_id} - {self.status}'

    def get_input_file_path(self):
        """
        Absolute path for input file
        """
        return os.path.join(settings.MEDIA_ROOT, self.input_file.name)

    def get_output_file_path(self):
        """
        Absolute path for output file
        """
        if self.output_file:
            return os.path.join(settings.MEDIA_ROOT, self.output_file.name)
        return None

    def get_key_file_path(self):
        """
        Absolute path for key file
        """
        if self.key_file:
            return os.path.join(settings.MEDIA_ROOT, self.key_file.name)
        return None

    def get_log_file_path(self):
        """
        Absolute path for log file
        """
        if self.log_file:
            return os.path.join(settings.MEDIA_ROOT, self.log_file.name)
        return None

    def failed(self):
        """
        Delete files and job if checks fail
        """
        input_path = self.get_input_file_path()
        output_path = self.get_output_file_path()
        job_folder = os.path.dirname(input_path)

        if os.path.exists(input_path):
            os.remove(input_path)

        if output_path and os.path.exists(output_path):
            os.remove(output_path)

        # remove job folder when empty
        if os.path.exists(job_folder) and not os.listdir(job_folder):
            os.rmdir(job_folder)

        # remove job after files cleanup
        self.delete()
