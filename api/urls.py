from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import DeidentificationJobViewSet, ResetJobsViewSet


router = DefaultRouter()
router.register(r'v1/jobs', DeidentificationJobViewSet)
router.register(r'v1/reset_jobs', ResetJobsViewSet, basename='reset-jobs')

urlpatterns = [
    path('', include(router.urls)),
]
