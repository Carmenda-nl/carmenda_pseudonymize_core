from django.http import FileResponse, HttpResponseNotFound
from django.conf import settings
from pathlib import Path
import mimetypes


class ServeMediaFilesMiddleware:
    """
    Additional middleware for data files in a production environment.
    Specifically designed by Django Heimgartner for use with
    PyInstaller-bundled applications.
    """

    def __init__(self, get_response):
        self.get_response = get_response

        # check if mime types are correct
        mimetypes.init()

        # assures if the `MEDIA_URL` ends with a slash
        if not settings.MEDIA_URL.endswith('/'):
            settings.MEDIA_URL = f'{settings.MEDIA_URL}/'

    def __call__(self, request):
        if request.path.startswith(settings.MEDIA_URL):
            relative_path = request.path[len(settings.MEDIA_URL):]

            # build a absolute file path
            file_path = Path(settings.MEDIA_ROOT) / relative_path

            # checks if the file exists and is readable
            if file_path.exists() and file_path.is_file():
                try:
                    file_obj = open(file_path, 'rb')
                    content_type, encoding = mimetypes.guess_type(
                        str(file_path))

                    if content_type is None:
                        content_type = 'application/octet-stream'

                    # sends the file as response
                    response = FileResponse(
                        file_obj, content_type=content_type)

                    if 'download' in request.GET:
                        response['Content-Disposition'] = f'attachment; filename="{file_path.name}"'

                    return response

                except (IOError, PermissionError):
                    # file can not be openend
                    return HttpResponseNotFound(f'Can not open the file: {relative_path}')

            return HttpResponseNotFound(f'File {relative_path} not found.')

        return self.get_response(request)
