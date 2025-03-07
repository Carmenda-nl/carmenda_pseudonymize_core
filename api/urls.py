from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import DeidentificationJobViewSet


router = DefaultRouter()
router.register(r'jobs', DeidentificationJobViewSet)

urlpatterns = [
    path('', include(router.urls)),
]
