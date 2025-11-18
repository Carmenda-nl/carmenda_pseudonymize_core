# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Serializers for API endpoints handling deidentification jobs."""

from __future__ import annotations

import re
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from django.core.files.uploadedfile import UploadedFile

from rest_framework import serializers

from api.models import DeidentificationJob
from api.utils import get_metadata
from core.utils.file_handling import check_file
from core.utils.progress_tracker import tracker


def _get_file_path(uploaded_file: UploadedFile) -> tuple[str, bool]:
    """Get file path from uploaded file, creating temporary file if needed."""
    if hasattr(uploaded_file, 'temporary_file_path'):
        return uploaded_file.temporary_file_path(), False

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        for chunk in uploaded_file.chunks():
            temp_file.write(chunk)

        return temp_file.name, True


def _validate_file(uploaded_file: UploadedFile, input_cols: str | None = None) -> UploadedFile:
    """Validate uploaded file has valid encoding, line endings, and separator."""
    file_path, temp_file = _get_file_path(uploaded_file)

    encoding, line_ending, separator = check_file(file_path)
    checks = {'encoding': encoding, 'line endings': line_ending, 'column separator': separator}
    missing = [check for check, value in checks.items() if not value]

    if missing:
        message = f'Could not determine file {", ".join(missing)}'
        raise serializers.ValidationError(message)

    if input_cols:
        input_cols_dict = dict(column.strip().split('=') for column in input_cols.split(','))

        with Path(file_path).open(encoding=encoding) as file:
            header = file.readline().strip()
            columns = [col.strip() for col in header.split(separator)]

            # Validate that specified columns exist in the file
            for col_value in input_cols_dict.values():
                if col_value not in columns:
                    available_cols = ', '.join(columns)
                    message = f'Column "{col_value}" not found in input file, available columns: {available_cols}'
                    raise serializers.ValidationError(message)

    # Clean up temporary file if we created one
    if temp_file and file_path:
        Path(file_path).unlink(missing_ok=True)

    return uploaded_file


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
            'zip_file',
            'zip_preview',
            'preview',
            'processed_preview',
            'status',
            'error_message',
        ]

    def validate_input_cols(self, value: str) -> str:
        """Validate that `input_cols` follows the required format.

        1. Comma-separated
        2. Each value follows the format: key=value
        3. Must contain the key `report`
        """
        if not isinstance(value, str):
            message = 'Input columns must be a string'
            raise serializers.ValidationError(message)

        fields = [field.strip() for field in value.split(',')]

        pattern = re.compile(r'^([^=]+)=(.+)$')
        field_dict = {}

        for field in fields:
            match = pattern.match(field)

            if not match:
                message = f"Field '{field}' does not follow the format 'key=value'"
                raise serializers.ValidationError(message)

            key = match.group(1)
            val = match.group(2)
            field_dict[key] = val

        if 'report' not in field_dict:
            message = "The 'report' key must be present (report=value)"
            raise serializers.ValidationError(message)

        return value

    def validate_input_file(self, value: UploadedFile) -> UploadedFile:
        """Validate the uploaded file and check column existence."""
        input_cols = self.initial_data.get('input_cols')
        return _validate_file(value, input_cols)

    def validate_datakey(self, value: UploadedFile) -> UploadedFile:
        """Validate the datakey if provided and valid."""
        if value:
            return _validate_file(value)
        return value

    def to_representation(self, instance: DeidentificationJob) -> dict:
        """Return the job including file metadata."""
        representation = super().to_representation(instance)
        fields = ['input_file', 'output_file', 'datakey', 'log_file', 'zip_file']

        return get_metadata(representation, instance, fields)


class JobStatusSerializer(serializers.ModelSerializer):
    """Provide detailed information about the current state of a job."""

    progress = serializers.SerializerMethodField()
    status = serializers.SerializerMethodField()

    class Meta:
        model = DeidentificationJob
        fields = '__all__'

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
