from rest_framework import serializers
from .models import DeidentificationJob


class DeidentificationJobSerializer(serializers.ModelSerializer):
    class Meta:
        model = DeidentificationJob
        fields = [
            'id', 'input_file', 'output_file', 'status', 'created_at',
            'updated_at', 'error_message', 'patient_name_column', 'time_column',
            'caretaker_name_column', 'report_column'
        ]

        read_only_fields = [
            'id',
            'output_file',
            'status',
            'created_at',
            'updated_at',
            'error_message'
        ]

    def create(self, validated_data):
        """
        Create a new deidentificatie job and return.
        """
        return DeidentificationJob.objects.create(**validated_data)
