from rest_framework.routers import DefaultRouter
from api.views import DeidentificationJobViewSet, ResetJobsViewSet
from django.urls import path, include
from drf_spectacular.views import SpectacularSwaggerView, SpectacularAPIView


router = DefaultRouter()
router.register('v1/jobs', DeidentificationJobViewSet, basename='jobs')
router.register('v1/reset', ResetJobsViewSet, basename='reset-jobs')

urlpatterns = [
    path('', include(router.urls)),
    path('docs/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
    path('schema/', SpectacularAPIView.as_view(), name='schema'),
]
