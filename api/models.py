from django.db import models
import uuid
import os


class DeidentificationJob(models.Model):
    STATUS_CHOICES = (
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    )

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    input_file = models.FileField(upload_to='uploads/')
    output_file = models.FileField(upload_to='results/', null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    error_message = models.TextField(blank=True, null=True)

    # metadata for colums
    patient_name_column = models.CharField(max_length=100, default='Cliëntnaam')
    time_column = models.CharField(max_length=100, default='Tijdstip')
    caretaker_name_column = models.CharField(max_length=100, default='Zorgverlener')
    report_column = models.CharField(max_length=100, default='rapport')

    def __str__(self):
        return f'Job {self.id} - {self.status}'

    def get_input_file_path(self):
        """Absolute path for input file."""
        return os.path.join('media', self.input_file.name)

    def get_output_file_path(self):
        """Absolute path for input file."""
        if self.output_file:
            return os.path.join('media', self.output_file.name)
        return None
