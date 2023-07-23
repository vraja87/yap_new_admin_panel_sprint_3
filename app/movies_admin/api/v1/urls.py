from django.urls import path

from movies_admin.api.v1 import views

urlpatterns = [
    path('movies/<uuid:pk>/', views.MoviesDetailApi.as_view()),
    path('movies/', views.MoviesListApi.as_view()),
]
