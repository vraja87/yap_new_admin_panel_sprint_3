from django.urls import path, include


urlpatterns = [
    path('v1/', include('movies_admin.api.v1.urls')),
]
