# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Serializers for API endpoints handling deidentification jobs."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from django.core.files.uploadedfile import UploadedFile

from rest_framework import serializers

from api.models import DeidentificationJob
from api.utils.file_handling import get_file_path, get_metadata
from api.utils.validators import validate_file, validate_file_columns, validate_input_cols
from core.utils.file_handling import check_file
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
            'output_datakey',
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
        """Validate the uploaded file basic checks.

        Skip validation:
            if this is an existing file path (PUT)
            if input_file is not in the request data (partial update)
        """
        if isinstance(value, str):
            return value

        if 'input_file' not in self.initial_data:
            return value

        # Validate file without input_cols check.
        validated_file, encoding, line_ending, separator = validate_file(value, input_cols=None)

        if not hasattr(self, '_file_metadata'):
            self._file_metadata = {}

        self._file_metadata['input_file'] = {
            'encoding': encoding,
            'line_ending': line_ending,
            'separator': separator,
            'uploaded_file': value,
        }

        return validated_file

    def validate_datakey(self, value: UploadedFile) -> UploadedFile | None:
        """Validate the datakey if provided and valid.

        Skip validation:
            if this is an existing file path (PUT)
            if datakey is not in the request data (partial update)

        Allow removal:
            if value is None or empty string, return None to clear the field
        """
        if isinstance(value, str):
            if not value:
                return None
            return value

        if 'datakey' not in self.initial_data:
            return value

        if value:
            validated_file, encoding, line_ending, separator = validate_file(value, datakey='datakey')

            if not hasattr(self, '_file_metadata'):
                self._file_metadata = {}
            self._file_metadata['datakey'] = {
                'encoding': encoding,
                'line_ending': line_ending,
                'separator': separator,
            }

            return validated_file
        return None

    def validate(self, attrs: dict) -> dict:
        """Cross-validate input_cols against input_file columns."""
        input_cols = attrs.get('input_cols')
        input_file = attrs.get('input_file')

        if 'input_cols' not in self.initial_data:
            return attrs

        if input_cols and input_file and not isinstance(input_file, str):
            metadata = getattr(self, '_file_metadata', {}).get('input_file', {})
            uploaded_file = metadata.get('uploaded_file')
            encoding = metadata.get('encoding')
            separator = metadata.get('separator')

            if uploaded_file and encoding and separator:
                file_path, is_temp = get_file_path(uploaded_file)

                try:
                    validate_file_columns(file_path, encoding, separator, input_cols)
                finally:
                    if is_temp and file_path:
                        Path(file_path).unlink(missing_ok=True)

        elif input_cols and self.instance and self.instance.input_file:
            file_path = self.instance.input_file.path
            encoding, _line_ending, separator = check_file(file_path)
            validate_file_columns(file_path, encoding, separator, input_cols)

        return attrs

    def to_representation(self, instance: DeidentificationJob) -> dict:
        """Return the job including file metadata."""
        representation = super().to_representation(instance)
        fields = ['input_file', 'output_file', 'datakey', 'output_datakey', 'log_file', 'error_rows_file', 'zip_file']

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
            stage = progress_info['stage']

            if stage:
                return str(stage)

        return obj.status

    def get_progress(self, obj: DeidentificationJob) -> int:
        """Get the current progress percentage from the tracker."""
        if obj.status == 'processing':
            progress_info = tracker.get_progress()
            percentage = progress_info['percentage']
            if percentage is None:
                return 0
            return int(percentage) if isinstance(percentage, str) else percentage

        return 100 if obj.status == 'completed' else 0
