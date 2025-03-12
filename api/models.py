from django.db import models
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
    input_file = models.FileField(upload_to='uploads')
    output_file = models.FileField(upload_to='results', null=True, blank=True)
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
        return os.path.join('media', self.input_file.name)

    def get_output_file_path(self):
        """
        Absolute path for input file
        """
        if self.output_file:
            return os.path.join('media', self.output_file.name)
        return None

    def failed(self):
        """
        Delete files and job if checks fail
        """
        if self.input_file:
            input_path = self.input_file.path

            if os.path.exists(input_path):
                os.remove(input_path)

        if self.output_file:
            output_path = self.output_file.path

            if os.path.exists(output_path):
                os.remove(output_path)

        # remove job after files cleanup
        self.delete()
