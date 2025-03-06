from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response


class ApiViewSet(viewsets.ViewSet):
    @action(methods=['GET'],  detail=False, name='Get Value from input')
    def get_val_from(self, request):

        input = request.GET['input']

        return Response(
            status=status.HTTP_200_OK, data=f'{input}, value from Django'
        )
