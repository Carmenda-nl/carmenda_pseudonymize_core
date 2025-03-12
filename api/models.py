from django.db import models
from django.conf import settings
import uuid
import os


def get_upload_to(instance, filename):
    """
    Build a path for the job files, based on the `job_id`
    """
    return os.path.join(str(instance.job_id), filename)


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
    input_file = models.FileField(upload_to=get_upload_to)
    output_file = models.FileField(upload_to=get_upload_to, null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    error_message = models.TextField(blank=True, null=True)

    # metadata for columns
    patient_column = models.CharField(max_length=100, default='patient')
    time_column = models.CharField(max_length=100, default='time')
    caretaker_column = models.CharField(max_length=100, default='caretaker')
    report_column = models.CharField(max_length=100, default='report')

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
