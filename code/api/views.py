from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.decorators import action
from drf_spectacular.utils import extend_schema_view, extend_schema, OpenApiResponse

from api.models import DeidentificationJob
from api.serializers import DeidentificationJobSerializer, ProcessJobSerializer, JobStatusSerializer
from api.manager import process_deidentification
import os

from django.conf import settings
import logging
import shutil


class ApiTags:
    """
    API tag constants for Swagger/OpenAPI documentation grouping.

    These tags are used to categorize endpoints in the API documentation
    for better organization and discoverability.
    """
    JOBS = 'Jobs'
    PROCESSING = 'Processing'
    CLEANUP = 'Cleanup'


@extend_schema(tags=[ApiTags.JOBS])
@extend_schema_view(
    process=extend_schema(tags=[ApiTags.PROCESSING]),
    check_status=extend_schema(tags=[ApiTags.PROCESSING]),
)
class DeidentificationJobViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing deidentification jobs.

    This ViewSet provides endpoints for creating, retrieving,
    and deleting deidentification jobs,
    as well as triggering processing and checking job status.
    """
    queryset = DeidentificationJob.objects.all()
    serializer_class = DeidentificationJobSerializer
    http_method_names = ['get', 'post', 'delete']

    def create(self, request, *args, **kwargs):
        """
        Prepares a new job with an uploaded file and column mapping configuration.
        The job is initialized with 'pending' status and awaits explicit processing.

        Required parameters:
            - input_file: The healthcare data file to be processed
            - input_cols: Mapping configuration specifying which columns to process

        Returns:
            HTTP_201_CREATED: Job created successfully
            HTTP_400_BAD_REQUEST: Validation errors in the request data
        """
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save(status='pending')

        headers = self.get_success_headers(serializer.data)
        return Response(
            serializer.data,
            status=status.HTTP_201_CREATED, headers=headers
        )

    @extend_schema(
        methods=['post'],
        request=ProcessJobSerializer,
        responses={
            200: OpenApiResponse(description='Job processing started successfully'),
            400: OpenApiResponse(description='Job cannot be processed or input file is missing'),
            500: OpenApiResponse(description='Failed to start job processing')
        }
    )
    @action(detail=True, methods=['post'])
    def process(self, request, pk=None):
        """
        Changes the job status to 'processing' and initiates the asynchronous
        deidentification process. Validates that the input file exists before starting.

        Parameters:
            pk (UUID): The job_id of the job to process

        Returns:
            HTTP_200_OK: Job processing started successfully
            HTTP_400_BAD_REQUEST: Job cannot be processed or input file is missing
            HTTP_500_INTERNAL_SERVER_ERROR: Failed to start job processing
        """
        job = self.get_object()

        # check if the input file exist
        if not job.input_file or not os.path.exists(job.input_file.path):
            return Response(
                {'Input file is missing'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # reset the status and error message on re-runs
        job.status = 'processing'
        job.error_message = None
        job.save()

        try:
            # start the anonymizing process
            process_deidentification(job.job_id)

            return Response(
                {'Job processing started successfully'},
                status=status.HTTP_200_OK
            )
        except Exception as e:
            job.status = 'failed'
            job.save()

            return Response(
                {'Failed to start job processing': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    @extend_schema(
        methods=['get'],
        responses={200: OpenApiResponse(JobStatusSerializer()), }
    )
    @action(detail=True, methods=['get'])
    def check_status(self, request, pk=None):
        """
        Returns the current status, progress percentage, and any error messages
        for the specified job.

        Parameters:
            pk (UUID): The job_id of the job to check

        Returns:
            HTTP_200_OK: Job status retrieved successfully, with status details in response body
        """
        job = self.get_object()
        serializer = JobStatusSerializer(job)

        return Response(
            serializer.data,
            status=status.HTTP_200_OK
        )


@extend_schema(tags=[ApiTags.CLEANUP])
class ResetJobsViewSet(viewsets.ViewSet):
    """
    This ViewSet provides administrative functionality to reset the system by
    deleting all job records and associated files. It should be used with caution
    and properly secured in production environments.

    Warnings:
        - This endpoint permanently deletes all job data
        - It should be secured with authentication in production
        - Use only for development, testing, or when a complete system reset is needed

    Endpoints:
        - DELETE /reset/select_all/: Delete all jobs and associated files
    """
    serializer_class = DeidentificationJobSerializer

    @extend_schema(
        methods=['delete'],
        responses={
            200: OpenApiResponse(description='Jobs successfully deleted'),
            500: OpenApiResponse(description='Error deleting jobs')
        }
    )
    @action(detail=False, methods=['delete'])
    def select_all(self, request):
        """
        Delete all jobs and associated files from the system.

        This operation:
        1. Removes all job records from the database
        2. Deletes all input and output files from storage
        3. Re-creates empty directories to maintain structure

        Warning: This action is irreversible and will permanently delete all job data.

        Returns:
            HTTP_200_OK: All jobs successfully deleted, with count of deleted jobs
            HTTP_500_INTERNAL_SERVER_ERROR: Error occurred during deletion
        """
        try:
            # get all jobs to track stats before deletion
            total_jobs = DeidentificationJob.objects.count()

            # get paths to input and output directories
            input_dir = os.path.join(settings.MEDIA_ROOT, 'input')
            output_dir = os.path.join(settings.MEDIA_ROOT, 'output')

            # ensure logger is not reserved before removal
            logging.shutdown()

            # remove all job-related files
            if os.path.exists(input_dir):
                shutil.rmtree(input_dir)
            if os.path.exists(output_dir):
                shutil.rmtree(output_dir)

            # delete all job records
            DeidentificationJob.objects.all().delete()

            # recreate empty directories to maintain structure
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(output_dir, exist_ok=True)

            return Response(
                f'All {total_jobs} jobs have been removed successfully.',
                status=status.HTTP_200_OK
            )

        except Exception as e:
            return Response(
                {'An error occurred while resetting jobs.\n': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
