from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from .models import DeidentificationJob
from .serializers import DeidentificationJobSerializer
from deduce_service.deduce_manager import process_deidentification
import threading


class DeidentificationJobViewSet(viewsets.ModelViewSet):
    queryset = DeidentificationJob.objects.all()
    serializer_class = DeidentificationJobSerializer

    def create(self, request, *args, **kwargs):
        """
        Override create method to accept file and run the job.
        """
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        job = serializer.save()

        # Start process in a seperate thread
        thread = threading.Thread(
            target=process_deidentification,
            args=(job.id,)
        )
        thread.daemon = True
        thread.start()

        headers = self.get_success_headers(serializer.data)
        return Response(
            serializer.data, status=status.HTTP_201_CREATED, headers=headers
        )

    @action(detail=True, methods=['get'])
    def check_status(self, request, pk=None):
        """Check the status of the job."""
        job = self.get_object()
        serializer = self.get_serializer(job)
        return Response(serializer.data)

    @action(detail=True, methods=['get'])
    def download(self, request, pk=None):
        """Download the results if available."""
        job = self.get_object()

        if job.status != 'completed' or not job.output_file:
            return Response(
                {"error": "Output file is not available yet"},
                status=status.HTTP_400_BAD_REQUEST
            )

        return Response({
            "download_url": request.build_absolute_uri(job.output_file.url)
        })
