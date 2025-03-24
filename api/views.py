from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.decorators import action

from .models import DeidentificationJob
from .serializers import DeidentificationJobSerializer
import os
import pandas

import threading
from services.manager import process_deidentification
from services.pyspark_deducer import main
# from services.deduce_manager_v1 import process_deidentification


class DeidentificationJobViewSet(viewsets.ModelViewSet):
    queryset = DeidentificationJob.objects.all()
    serializer_class = DeidentificationJobSerializer

    def create(self, request, *args, **kwargs):
        """
        Override create method to accept CSV or Parquet file
        and extract column names before running the job
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

        # get file columns and check if they are proper
        try:
            # input_file_path = os.path.abspath(job.get_input_file_path())

            # if file_extension == '.csv':
            #     df = pandas.read_csv(input_file_path, nrows=1)  # only read the first row
            # elif file_extension == '.parquet':
            #     df = pandas.read_parquet(input_file_path, engine='pyarrow')

            # required_columns = 4
            # if len(df.columns) < required_columns or any(col.strip() == '' for col in df.columns[:required_columns]):
            #     job.failed()
            #     return Response({'File must contain at least 4 named columns.'}, status=status.HTTP_400_BAD_REQUEST)

            # job.patient_column = df.columns[0]
            # job.time_column = df.columns[1]
            # job.caretaker_column = df.columns[2]
            # job.report_column = df.columns[3]

            job.save()

        except Exception as exception:
            job.failed()
            return Response({f'Failed to process file: {str(exception)}'}, status=status.HTTP_400_BAD_REQUEST)

        # start the anonymizing process in a seperate thread
        # thread = threading.Thread(
        #     target=process_deidentification,
        #     args=(job.job_id,)
        # )
        thread = threading.Thread(
            target=main,
            args=(
                # job.input_file.path,                  # input_path
                f"/tmp/deidentified_{job.job_id}.csv",  # output_path
                ["patient_id", "timestamp", "caregiver", "report"],  # input_cols
                ["anon_patient", "timestamp", "anon_caregiver", "report"],  # output_cols
                "my_secret_key",  # pseudonym_key
                4,  # max_n_processes
                "csv",  # output_extension
                1,  # partition_n
                1   # coalesce_n
            )
        )
        thread.daemon = True
        thread.start()

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
