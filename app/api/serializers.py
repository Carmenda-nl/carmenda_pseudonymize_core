from rest_framework import serializers
from api.models import DeidentificationJob
from pseudonymize.services.progress_tracker import tracker
import re


class DeidentificationJobSerializer(serializers.ModelSerializer):
    """
    This serializer validates job configuration parameters, particularly the input_cols
    field which must follow a specific format. It defines which fields are read-only
    and implements custom validation logic for the input column mapping.

    Attributes:
        Meta: Configures the serializer with model and field specifications.
    """
    class Meta:
        model = DeidentificationJob
        fields = '__all__'
        read_only_fields = [
            'job_id', 'output_file', 'key_file', 'log_file', 'zip_file', 'zip_preview',
            'processed_preview', 'status', 'created_at', 'updated_at', 'error_message'
        ]

    def validate_input_cols(self, value):
        """
        Validates that `input_cols` is:
          1. Comma-separated
          2. Each value follows the format: key=value
          3. Contains at least one of the fields `patientName` or `report`

        Args:
            value (str): The input_cols value to validate

        Returns:
            str: The validated input_cols value

        Raises:
            ValidationError: If validation fails for any of the rules
        """
        if not isinstance(value, str):
            raise serializers.ValidationError('Input columns must be a string')

        # split by comma and remove whitespace
        fields = [field.strip() for field in value.split(',')]

        pattern = re.compile(r'^([^=]+)=(.+)$')
        field_dict = {}

        for field in fields:
            match = pattern.match(field)

            if not match:
                raise serializers.ValidationError(
                    f"Field '{field}' does not follow the format 'key=value'"
                )
            key = match.group(1)
            val = match.group(2)
            field_dict[key] = val

        # check that at least one of the required fields is present
        if 'patientName' not in field_dict and 'report' not in field_dict:
            raise serializers.ValidationError("At least one of 'patientName' or 'report' must be present")

        return value

    def create(self, validated_data):
        """
        After successful validation, creates and persists
        a new job in the database.

        Args:
            validated_data (dict): Data that has passed validation

        Returns:
            DeidentificationJob: The newly created job instance
        """
        return DeidentificationJob.objects.create(**validated_data)


class ProcessJobSerializer(serializers.ModelSerializer):
    """
    This lightweight serializer is used when a job is submitted for processing,
    only exposing the necessary `input_cols` field.

    Attributes:
        Meta: Configures the serializer with model and field specifications.
    """
    class Meta:
        model = DeidentificationJob
        fields = ['input_cols']


class JobStatusSerializer(serializers.ModelSerializer):
    """
    Provides information about the current state of a deidentification job,
    including its status, progress percentage, and any error messages.

    Attributes:
        progress (SerializerMethodField): Calculated field for job progress.
        Meta: Configures the serializer with model and field specifications.
    """
    progress = serializers.SerializerMethodField()

    class Meta:
        model = DeidentificationJob
        fields = ['status', 'progress', 'error_message']

    def get_progress(self, obj):
        """
        Uses the global tracker instance to get the current progress value.

        Args:
            obj (DeidentificationJob): The job instance being serialized

        Returns:
            int: Progress percentage from 0-100
        """
        return tracker.get_progress()
