from rest_framework.routers import DefaultRouter
from api.views import DeidentificationJobViewSet, ResetJobsViewSet
from django.urls import path, include
from django.conf import settings


router = DefaultRouter()
router.register('v1/jobs', DeidentificationJobViewSet, basename='jobs')
router.register('v1/reset', ResetJobsViewSet, basename='reset-jobs')

urlpatterns = [
    path('', include(router.urls)),
]

if settings.DEBUG:
    from drf_spectacular.views import SpectacularSwaggerView, SpectacularAPIView

    urlpatterns += [
        path('docs/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
        path('schema/', SpectacularAPIView.as_view(), name='schema'),
    ]
