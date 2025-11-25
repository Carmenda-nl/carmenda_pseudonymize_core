# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Serializers for API endpoints handling deidentification jobs."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from django.core.files.uploadedfile import UploadedFile

from rest_framework import serializers

from api.models import DeidentificationJob
from api.utils import get_metadata, validate_file, validate_input_cols
from core.utils.progress_tracker import tracker


class DeidentificationJobListSerializer(serializers.ModelSerializer):
    """Listing jobs with basic status information."""

    details_url = serializers.SerializerMethodField()
    has_datakey = serializers.SerializerMethodField()

    class Meta:
        model = DeidentificationJob
        fields: ClassVar = ['job_id', 'details_url', 'has_datakey', 'status']
        read_only_fields: ClassVar = ['job_id', 'status']

    def get_details_url(self, obj: DeidentificationJob) -> str:
        """URL for the job details and processing information."""
        request = self.context.get('request')

        if request:
            return request.build_absolute_uri(f'/api/v1/jobs/{obj.job_id}')
        return f'/api/v1/jobs/{obj.job_id}'

    def get_has_datakey(self, obj: DeidentificationJob) -> bool:
        """Check if a datakey file has been provided for this job."""
        return bool(obj.datakey)


class DeidentificationJobSerializer(serializers.ModelSerializer):
    """Validate job configuration parameters and handle deidentification job data."""

    class Meta:
        model = DeidentificationJob
        fields = '__all__'
        read_only_fields: ClassVar = [
            'job_id',
            'output_file',
            'log_file',
            'error_rows_file',
            'zip_file',
            'zip_preview',
            'preview',
            'processed_preview',
            'status',
            'error_message',
        ]

    def validate_input_cols(self, value: str) -> str:
        """Validate input_cols on proper format, skip when empty."""
        if not value:
            return value

        return validate_input_cols(value)

    def validate_input_file(self, value: UploadedFile) -> UploadedFile:
        """Validate the uploaded file and check column existence.

        Skip validation:
            if this is an existing file path (PUT)
        """
        if isinstance(value, str):
            return value

        input_cols = self.initial_data.get('input_cols')
        return validate_file(value, input_cols)

    def validate_datakey(self, value: UploadedFile) -> UploadedFile:
        """Validate the datakey if provided and valid.

        Skip validation:
            if this is an existing file path (PUT)
        """
        if isinstance(value, str):
            return value

        if value:
            return validate_file(value, datakey='datakey')
        return value

    def to_representation(self, instance: DeidentificationJob) -> dict:
        """Return the job including file metadata."""
        representation = super().to_representation(instance)
        fields = ['input_file', 'output_file', 'datakey', 'log_file', 'error_rows_file', 'zip_file']

        return get_metadata(representation, instance, fields)


class JobStatusSerializer(serializers.ModelSerializer):
    """Provide detailed information about the current state of a job."""

    progress = serializers.SerializerMethodField()
    status = serializers.SerializerMethodField()

    class Meta:
        model = DeidentificationJob
        fields: ClassVar = ['job_id', 'status', 'progress', 'error_message']
        read_only_fields: ClassVar = ['job_id', 'status', 'progress', 'error_message']

    def get_status(self, obj: DeidentificationJob) -> str:
        """Get the combined status and stage information."""
        if obj.status == 'processing':
            progress_info = tracker.get_progress()

            if progress_info['stage']:
                return progress_info['stage']

        return obj.status

    def get_progress(self, obj: DeidentificationJob) -> int:
        """Get the current progress percentage from the tracker."""
        if obj.status == 'processing':
            progress_info = tracker.get_progress()
            return progress_info['percentage']

        return 100 if obj.status == 'completed' else 0
