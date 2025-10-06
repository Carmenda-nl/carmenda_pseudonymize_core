# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Serializers for API endpoints handling deidentification jobs.

This module contains serializers for managing deidentification jobs,
including validation, processing, and status tracking functionality.
"""

import re
from typing import ClassVar

from rest_framework import serializers

from api.models import DeidentificationJob
from utils.progress_tracker import tracker


class DeidentificationJobListSerializer(serializers.ModelSerializer):
    """Lightweight serializer for listing jobs with minimal information."""

    process_url = serializers.SerializerMethodField()

    class Meta:
        """Set model and field specifications for the serializer."""

        model = DeidentificationJob
        fields: ClassVar = ['job_id', 'process_url', 'status']
        read_only_fields: ClassVar = ['job_id', 'status']

    def get_process_url(self, obj: DeidentificationJob) -> str:
        """Generate the process URL for the job."""
        request = self.context.get('request')
        if request:
            return request.build_absolute_uri(f'/api/v1/jobs/{obj.job_id}/process/')
        return f'/api/v1/jobs/{obj.job_id}/process/'


class DeidentificationJobSerializer(serializers.ModelSerializer):
    """Validate job configuration parameters and handle deidentification job data."""

    class Meta:
        """Set model and field specifications for the serializer."""

        model = DeidentificationJob
        fields = '__all__'
        read_only_fields: ClassVar = [
            'job_id',
            'output_file',
            'log_file',
            'zip_file',
            'zip_preview',
            'processed_preview',
            'status',
            'error_message',
        ]

    def validate_input_cols(self, value: str) -> str:
        """Validate that `input_cols` follows the required format.

        1. Comma-separated
        2. Each value follows the format: key=value
        3. Contains at least one of the fields `patientName` or `report`
        """
        if not isinstance(value, str):
            msg = 'Input columns must be a string'
            raise serializers.ValidationError(msg)

        # Split by comma and remove whitespace
        fields = [field.strip() for field in value.split(',')]

        pattern = re.compile(r'^([^=]+)=(.+)$')
        field_dict = {}

        for field in fields:
            match = pattern.match(field)

            if not match:
                msg = f"Field '{field}' does not follow the format 'key=value'"
                raise serializers.ValidationError(msg)

            key = match.group(1)
            val = match.group(2)
            field_dict[key] = val

        # Check that at least one of the required fields is present
        if 'patientName' not in field_dict and 'report' not in field_dict:
            msg = "At least one of 'patientName' or 'report' must be present"
            raise serializers.ValidationError(msg)

        return value

    def create(self, validated_data: dict) -> DeidentificationJob:
        """Create and persist a new job in the database.

        After successful validation, creates and persists
        a new job in the database.
        """
        return DeidentificationJob.objects.create(**validated_data)


class ProcessJobSerializer(serializers.ModelSerializer):
    """Lightweight serializer used when a job is submitted for processing."""

    class Meta:
        """Set model and field specifications for the serializer."""

        model = DeidentificationJob

        # Only exposes the necessary `input_cols` field.
        fields: ClassVar = ['input_cols']


class JobStatusSerializer(serializers.ModelSerializer):
    """Provide information about the current state of a deidentification job.

    Includes its status, progress percentage, and any error messages.
    """

    progress = serializers.SerializerMethodField()
    status = serializers.SerializerMethodField()

    class Meta:
        """Set model and field specifications for the serializer."""

        model = DeidentificationJob
        fields: ClassVar = [
            'status',
            'progress',
            'error_message',
            'input_file',
            'output_file',
            'key_file',
            'log_file',
            'zip_file',
            'zip_preview',
            'processed_preview',
        ]

    def get_progress(self, obj: DeidentificationJob) -> int:
        """Get the current progress percentage from the database or tracker."""
        # Use database value if available, otherwise fall back to tracker
        if obj.progress_percentage > 0:
            return obj.progress_percentage

        progress_info = tracker.get_progress()
        return progress_info['percentage']

    def get_status(self, obj: DeidentificationJob) -> str:
        """Get the combined status and stage information."""
        # Use database value if available
        if obj.current_stage:
            return obj.current_stage

        # During processing, fall back to tracker for live updates
        if obj.progress_status == 'processing':
            progress_info = tracker.get_progress()
            if progress_info['stage']:
                return progress_info['stage']

        # Otherwise return the progress status
        return obj.progress_status
