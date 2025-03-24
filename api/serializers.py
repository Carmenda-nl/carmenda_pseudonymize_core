from rest_framework import serializers
from .models import DeidentificationJob


class DeidentificationJobSerializer(serializers.ModelSerializer):
    """
    Model serializer set for REST api
    """
    class Meta:
        model = DeidentificationJob

        fields = [
            'job_id',
            'input_file',
            'output_file',
            'status',
            'created_at',
            'updated_at',
            'error_message',
            # 'patient_column',
            # 'time_column',
            # 'caretaker_column',
            # 'report_column'
        ]

        read_only_fields = [
            'job_id',
            'output_file',
            'status',
            'created_at',
            'updated_at',
            'error_message'
        ]

    def create(self, validated_data):
        """
        Create a new deidentification job
        """
        return DeidentificationJob.objects.create(**validated_data)
