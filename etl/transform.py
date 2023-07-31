from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class ActorsWriters(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str


class DbFilmPerson(BaseModel):
    id: Optional[UUID] = Field(default_factory=uuid4)
    fw_id: UUID = Field(default_factory=uuid4)
    created: datetime
    description: Optional[str]
    full_name: Optional[str]
    modified: datetime
    name: Optional[str]
    rating: Optional[float]
    role: Optional[str]
    title: str
    type: str


class EsFilm(BaseModel):
    """
    схема ElasticSearch нужные данные:

    id - изменение в фильме. надо подтянуть все связанные данные
    imdb_rating
    genre - МАССИВ - изменение в жанре. затронет все связанные фильмы.
    title - title.raw
    description - МАССИВ
    director
    actors_names
    writers_names
    actors{
        id, name
    }
    writers{
        id, name
    }
    """
    id: UUID = Field(default_factory=uuid4)
    imdb_rating: Optional[float]
    genre: list
    title: str
    description: Optional[str]
    director: list
    actors_names: list[str]
    writers_names: list[str]
    actors: list[ActorsWriters]
    writers: list[ActorsWriters]


class Transform:
    elastic_format: dict[UUID, EsFilm]
    raw_films_linked: list[dict]
    films_linked: list[DbFilmPerson]

    def __init__(self, films_linked: list[dict]) -> None:
        self.raw_films_linked = films_linked
        self.films_linked = [DbFilmPerson.model_validate(film_dict)
                             for film_dict in films_linked]

    def reformat(self) -> None:
        """Приводим данные ближе к формату elasticsearch"""
        step_one: dict[UUID, dict] = {}
        # первый этап. укладываем данные ближе к формату эластик.
        for one_db_film in self.films_linked:
            step_one.setdefault(one_db_film.fw_id, {
                'genre': set(),
                'director': set(),
                'actors': set(),
                'writers': set(),
            })
            step_one[one_db_film.fw_id]['imdb_rating'] = one_db_film.rating
            step_one[one_db_film.fw_id]['title'] = one_db_film.title
            step_one[one_db_film.fw_id]['title.raw'] = one_db_film.title
            step_one[one_db_film.fw_id][
                'description'] = one_db_film.description

            step_one[one_db_film.fw_id]['genre'].add(one_db_film.name)

            if one_db_film.role == 'director':
                step_one[one_db_film.fw_id]['director'].add(
                    one_db_film.full_name)  # директорам id не нужен
            if one_db_film.role == 'actor':
                step_one[one_db_film.fw_id]['actors'].add(
                    (one_db_film.id, one_db_film.full_name)
                )
            if one_db_film.role == 'writer':
                step_one[one_db_film.fw_id]['writers'].add(
                    (one_db_film.id, one_db_film.full_name)
                )
        # второй этап. укладываем данные ближе к формату эластик.
        self.elastic_format = {}
        for fw_id, film_dict in step_one.items():
            film_dict['actors'] = [{'id': uuid, 'name': name} for uuid, name
                                   in film_dict['actors']]
            film_dict['writers'] = [{'id': uuid, 'name': name} for uuid, name
                                    in film_dict['writers']]
            film_dict['actors_names'] = list(map(lambda x: x['name'],
                                                 film_dict['actors']))
            film_dict['writers_names'] = list(map(lambda x: x['name'],
                                                  film_dict['writers']))
            film_dict['id'] = fw_id
            self.elastic_format[fw_id] = EsFilm.model_validate(film_dict)
