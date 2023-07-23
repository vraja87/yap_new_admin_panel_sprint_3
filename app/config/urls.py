import os

import debug_toolbar
from django.contrib import admin
from django.urls import include, path

DEBUG = os.environ.get('DEBUG', False) == 'True'

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', include('movies_admin.api.urls')),
]
if DEBUG:
    urlpatterns.append(path('__debug__/', include(debug_toolbar.urls)))
