from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.decorators import action

from .models import DeidentificationJob
from .serializers import DeidentificationJobSerializer
from services.manager import process_deidentification
import os

import shutil
from django.conf import settings


class DeidentificationJobViewSet(viewsets.ModelViewSet):
    """
    Override the default create method to handle file-based
    deidentification job creation.
    """
    queryset = DeidentificationJob.objects.all()
    serializer_class = DeidentificationJobSerializer

    def create(self, request, *args, **kwargs):
        """
        Override create method to accept CSV or Parquet file
        """
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        job = serializer.save()

        # check if filetype is allowed
        file_extension = os.path.splitext(job.input_file.name)[1].lower()
        allowed_extensions = {'.csv', '.parquet'}

        # extension is not allowed
        if file_extension not in allowed_extensions:
            job.failed()
            return Response(
                {f'The file type {file_extension} is not supported. Try another file type.'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # start the anonymizing process in a seperate thread
        process_deidentification(job.job_id)

        headers = self.get_success_headers(serializer.data)
        return Response(serializer.data, status=status.HTTP_201_CREATED, headers=headers)

    @action(detail=True, methods=['get'])
    def check_status(self, request, pk=None):
        """
        Check the status of the job
        """
        job = self.get_object()
        serializer = self.get_serializer(job)
        return Response(serializer.data)

    @action(detail=True, methods=['get'])
    def download(self, request, pk=None):
        """
        Download the results if available
        """
        job = self.get_object()

        if job.status != 'completed' or not job.output_file:
            return Response({'Output file is not available yet'}, status=status.HTTP_400_BAD_REQUEST)

        return Response({'download_url': request.build_absolute_uri(job.output_file.url)})


class ResetJobsViewSet(viewsets.ViewSet):
    """
    This ViewSet provides a custom action to:
        1. Delete all job records from the database
        2. Remove all associated input and output files
        3. Clean up any empty directories

    Don't use this in a webapp setup or secure it with a login.
    """
    @action(detail=False, methods=['post'])
    def reset(self, request):
        try:
            # get all jobs to track stats before deletion
            total_jobs = DeidentificationJob.objects.count()

            # get paths to input and output directories
            input_dir = os.path.join(settings.MEDIA_ROOT, 'input')
            output_dir = os.path.join(settings.MEDIA_ROOT, 'output')

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

            return Response({
                'message': 'All jobs have been reset successfully.',
                'total_jobs_deleted': total_jobs
            }, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({
                'error': 'An error occurred while resetting jobs.',
                'details': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
