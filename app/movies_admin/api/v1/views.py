from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Q
from django.http import JsonResponse
from django.views.generic import DetailView
from django.views.generic.list import BaseListView

from movies_admin.models import Filmwork, PersonFilmwork


class MoviesApiMixin:
    model = Filmwork
    http_method_names = ['get']

    def get_queryset(self):
        queryset = self.model.objects.prefetch_related(
            'genres', 'persons').annotate(writers=ArrayAgg(
            "persons__full_name", filter=Q(
                personfilmwork__role=PersonFilmwork.RoleType.WRITER),
            distinct=True)).annotate(
            directors=ArrayAgg("persons__full_name", filter=Q(
                personfilmwork__role=PersonFilmwork.RoleType.DIRECTOR),
                               distinct=True)).annotate(
            actors=ArrayAgg("persons__full_name", filter=Q(
                personfilmwork__role=PersonFilmwork.RoleType.ACTOR),
                            distinct=True)).values().annotate(
            genres=ArrayAgg("genrefilmwork__genre__name", distinct=True))
        return queryset

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)


class MoviesListApi(MoviesApiMixin, BaseListView):
    model = Filmwork
    http_method_names = ['get']  # Список методов, которые реализует обработчик
    paginate_by = 50

    def get_context_data(self, *, object_list=None, **kwargs):
        queryset = self.get_queryset()

        paginator, page, queryset, is_paginated = self.paginate_queryset(
            queryset,
            self.paginate_by
        )
        context = {
            "count": paginator.count,
            "total_pages": paginator.num_pages,
            "prev": page.previous_page_number() if page.has_previous() else None,
            "next": page.next_page_number() if page.has_next() else None,
            "results": list(queryset),
        }
        return context


class MoviesDetailApi(MoviesApiMixin, DetailView):

    def get_context_data(self, *, object_list=None, **kwargs):
        return self.get_object()
