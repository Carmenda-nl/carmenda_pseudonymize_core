from django.db import models
import uuid


class DeidentificationJob(models.Model):
    """
    Tracking the processing of healthcare data files
    through a deidentification pipeline,
    maintaining state information and file references.
    """
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
    key_file = models.FileField(upload_to='output', null=True, blank=True)
    log_file = models.FileField(upload_to='output', null=True, blank=True)
    zip_file = models.FileField(upload_to='output', null=True, blank=True)
    zip_preview = models.JSONField(null=True, blank=True)
    processed_preview = models.JSONField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    error_message = models.TextField(blank=True, null=True)

    def __str__(self):
        return f'Job {self.job_id} - {self.status}'
