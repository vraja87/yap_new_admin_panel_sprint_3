import abc
import datetime
import json
import os
from time import sleep
from typing import Any, Optional, Tuple
from uuid import uuid4, UUID

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from functools import wraps

from os import environ
from dotenv import load_dotenv

import logging

import psycopg2
from psycopg2.extras import RealDictCursor

from typing import Dict, List
from pydantic import BaseModel, Field
from django.core.serializers.json import DjangoJSONEncoder
from dateutil.parser import parser


load_dotenv()


def backoff(start_sleep_time: float = 0.1, factor: int = 2,
            border_sleep_time: int = 10):
    """Повторение исполнения метода.

    Через некоторое время, при практически любом эксепшне.

    Использует наивный экспоненциальный рост времени повтора (factor)
    до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            n, timeout = 1, start_sleep_time
            while True:
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    logger.error(
                        f'Ошибка БД. Выполнение {func.__name__}. Backoff {n}.'
                        f' Описание:{e}'
                    )
                    sleep(timeout)
                    timeout = start_sleep_time * factor ** n \
                        if timeout < border_sleep_time else border_sleep_time
                    n += 1
        return inner
    return func_wrapper


def write_operations_state():
    """Декоратор сохранения состояние метода класса и её результата.

    Формат имени - имя_класса.имя_метода.ключ .
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(self, *args, **kwargs):
            self.state.set_state(
                f'{self.__class__.__name__}.{func.__name__}',
                CacheStates.START
            )
            result = func(self, *args, **kwargs)
            self.state.set_state(
                f'{self.__class__.__name__}.{func.__name__}',
                CacheStates.FINISH
            )
            self.state.set_state(
                f'{self.__class__.__name__}.{func.__name__}.result',
                result
            )
            return result
        return inner
    return func_wrapper


class PostgresSaver(object):
    """Класс работы c postgresql, запись/чтение(для тестов)."""

    dsl_dict: Dict

    def __init__(self, dsl_dict=None):
        """На вход - коннект к базе"""

        self.dsl_dict = {
            'dbname': environ.get('DB_NAME'),
            'user': environ.get('DB_USER'),
            'password': environ.get('DB_PASSWORD'),
            'host': environ.get('DB_HOST'),
            'port': environ.get('DB_PORT'),
        } if dsl_dict is None else dsl_dict
        self.connect()

    @backoff()
    def connect(self):
        """Устанавливаем соединение с базой"""
        self.connection = psycopg2.connect(
            **self.dsl_dict, cursor_factory=RealDictCursor,
        )
        self.cursor = self.connection.cursor()
        return True

    def execute(self, query: str) -> List:
        """Собираем выборку с базы."""
        try:
            self.cursor.execute(query)
        except (psycopg2.Error, psycopg2.Warning) as exc:
            self.cursor.close()
            self.connection.close()
            raise exc
        raw_data = self.cursor.fetchall()
        return [dict(row) for row in raw_data]

    def disconnect(self):
        self.cursor.close()
        self.connection.commit()
        self.connection.close()

    def __del__(self):
        """Закрываем соединение, при удалении объекта."""
        try:
            if self.connection is not None:
                self.disconnect()
        except Exception as e:
            pass


def get_logger(logger_name):
    # type: (str) -> logging.Logger
    format_ = '%(asctime)s %(levelname)s ' \
             'PID[%(process)d] %(module)s: %(message)s'
    logger_ = logging.getLogger(logger_name)
    logger_.setLevel(logging.INFO)
    if not logger_.handlers:
        fh = logging.FileHandler(os.environ.get('ETL_LOG',
                                                './log/etl.log'))
        formatter = logging.Formatter(format_)
        fh.setFormatter(formatter)
        logger_.addHandler(fh)
    return logger_


logger = get_logger('etl module')


class CacheStates(object):
    FINISH = 'finish'
    START = 'start'
    ERROR = 'error'


class PostgresMixin(abc.ABC):
    """Общий код классов работы с базой."""
    max_modified_after: Optional[datetime.datetime]
    modified_after: Optional[datetime.datetime]
    has_results: bool
    path: str
    results: Dict

    def __init__(self, modified_after):
        self.storage = JsonFileStorage(self.path)
        self.state = State(self.storage)
        self.results = {}
        self.has_results = False

        self.modified_after = self.max_modified_after = modified_after

    @staticmethod
    def get_max_modified(ready_result) -> datetime.datetime:
        """Возвращает максимальное время для поля modified"""
        a = []
        for res in ready_result:
            if isinstance(res['modified'], datetime.datetime):
                a.append(res['modified'])
            else:
                a.append(parser().parse(res['modified']))
        return max(a)

    def analyze_result(self, result):
        """Определяем есть результат сбора и обновляем максимальную дату"""
        if len(result) > 0:
            self.has_results = True
        else:
            return  # иначе max выдаст еррор
        max_modified = self.get_max_modified(result)
        self.max_modified_after = max_modified \
            if max_modified > self.max_modified_after \
            else self.max_modified_after

    @abc.abstractmethod
    def _collect_methods(self) -> Tuple:
        """Возвращает кортеж методов для выполнения collect"""

    def collect(self):
        """Общий метод сбора, с обработкой кэшей.

        Оперирует только статусами start/finish, error не используется.
        Линейно. В случае ошибки если состояние ФИНИШ,-
        читаем его результат из кэша.
        Если СТАРТ,- значит он не закончился, надо его и выполнить.
        """
        get_methods = self._collect_methods()

        self.results = {}

        is_broken = False  # было поломано выполнение класса
        found_broken = False  # нашли на каком методе вывалилось выполнение

        global_state = self.state.get_state(f'{self.__class__.__name__}')
        if global_state == CacheStates.START:  # значит вывалились в процессе
            is_broken = True

        self.state.set_state(f'{self.__class__.__name__}', CacheStates.START)

        for method in get_methods:
            state = self.state.get_state(
                f'{self.__class__.__name__}.{method.__name__}'
            )

            if not is_broken or found_broken:
                self.results[method.__name__] = method()
                self.analyze_result(self.results[method.__name__])
                continue

            if state == CacheStates.START:
                found_broken = True  # нашёл ломаного
                self.results[method.__name__] = method()
                self.analyze_result(self.results[method.__name__])
                continue

            self.results[method.__name__] = self.state.get_state(
                f'{self.__class__.__name__}.{method.__name__}.result'
            )
            self.analyze_result(self.results[method.__name__])

        self.state.set_state(f'{self.__class__.__name__}', CacheStates.FINISH)


class PostgresProducer(PostgresMixin):
    """Собирает с базы инфу по фильмамю"""
    postgres_saver: PostgresSaver
    limit_size: int
    modified_after: datetime.datetime
    max_modified_after: Optional[datetime.datetime]
    n_run: int

    path = './cache/postgres_producer.txt'

    def __init__(self, postgres_saver, limit_size, modified_after, n_run):
        super(PostgresProducer, self).__init__(modified_after)
        self.postgres_saver = postgres_saver
        self.limit_size = limit_size

        self.n_run = n_run
        self.offset_size = limit_size * (n_run - 1)

    @write_operations_state()
    def get_person(self):
        query = f"""
            SELECT id, modified
            FROM content.person
            WHERE modified > '{self.modified_after}'
            ORDER BY modified
            LIMIT {self.limit_size} OFFSET {self.offset_size};"""
        result = self.postgres_saver.execute(query)
        return result

    @write_operations_state()
    def get_genre(self):
        query = f"""
            SELECT id, modified
            FROM content.genre
            WHERE modified > '{self.modified_after}'
            ORDER BY modified
            LIMIT {self.limit_size} OFFSET {self.offset_size};"""
        result = self.postgres_saver.execute(query)
        return result

    @write_operations_state()
    def get_filmwork(self):
        query = f"""
            SELECT id, modified
            FROM content.film_work
            WHERE modified > '{self.modified_after}'
            ORDER BY modified
            LIMIT {self.limit_size} OFFSET {self.offset_size};"""
        result = self.postgres_saver.execute(query)
        return result

    def _collect_methods(self):
        return self.get_person, self.get_genre, self.get_filmwork


class PostgresEnricher(PostgresMixin):
    """Дополняет инфу по фильмам, инфой о актёрах и жанрах"""
    limit_size: int
    path = './cache/postgres_enricher.txt'

    def __init__(self, ready_producer: PostgresProducer, limit_size,
                 modified_after, n):
        super(PostgresEnricher, self).__init__(modified_after)
        self.ready_producer = ready_producer
        self.postgres_saver = ready_producer.postgres_saver
        self.all_persons_uuid = list(map(lambda x: x['id'],
                                         ready_producer.results['get_person']))
        self.all_genres_uuid = list(map(lambda x: x['id'],
                                        ready_producer.results['get_genre']))

        self.limit_size = limit_size
        self.n = n
        self.offset_size = limit_size * (n - 1)

    @write_operations_state()
    def get_person_links(self):
        if not self.all_persons_uuid:
            return []
        all_uuid_str = ','.join(map(lambda x: f"'{x}'",
                                    self.all_persons_uuid))
        query = f"""
        SELECT fw.id, fw.modified
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        WHERE pfw.person_id IN ({all_uuid_str})
        ORDER BY fw.modified
        LIMIT {self.limit_size} OFFSET {self.offset_size}
        ;"""
        result = self.postgres_saver.execute(query)
        return result

    @write_operations_state()
    def get_genre_links(self):
        if not self.all_genres_uuid:
            return []
        all_uuid_str = ','.join(map(lambda x: f"'{x}'",
                                    self.all_genres_uuid))
        query = f"""
        SELECT fw.id, fw.modified
        FROM content.film_work fw
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        WHERE gfw.genre_id IN ({all_uuid_str})
        ORDER BY fw.modified
        LIMIT {self.limit_size} OFFSET {self.offset_size}
        ;"""
        result = self.postgres_saver.execute(query)
        return result

    def _collect_methods(self):
        return self.get_person_links, self.get_genre_links


class PostgresMerger(PostgresMixin):

    path = './cache/postgres_merger.txt'

    def __init__(self, postgres_saver: PostgresSaver, modified_after,
                 genre_results, person_results):
        super(PostgresMerger, self).__init__(modified_after)
        self.postgres_saver = postgres_saver
        self.genre_results = genre_results
        self.person_results = person_results

    @write_operations_state()
    def get_films_linked(self):
        films_via_genre = list(map(lambda x: x['id'], self.genre_results))
        films_via_person = list(map(lambda x: x['id'], self.person_results))
        films_uuid = set(films_via_genre + films_via_person)
        if not films_uuid:
            return []

        all_uuid_str = ','.join(map(lambda x: f"'{x}'", films_uuid))
        query = f"""
        SELECT
            fw.id as fw_id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            fw.created,
            fw.modified,
            pfw.role,
            p.id,
            p.full_name,
            g.name
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id IN ({all_uuid_str});"""
        result = self.postgres_saver.execute(query)
        return result

    def _collect_methods(self) -> Tuple:
        return self.get_films_linked,


class DbFilmPerson(BaseModel):
    id: Optional[UUID] = Field(default_factory=uuid4)
    fw_id: UUID = Field(default_factory=uuid4)
    created: datetime.datetime
    description: Optional[str]
    full_name: Optional[str]
    modified: datetime.datetime
    name: Optional[str]
    rating: Optional[float]
    role: Optional[str]
    title: str
    type: str


class ActorsWriters(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str


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
    genre: List
    title: str
    description: Optional[str]
    director: List
    actors_names: List[str]
    writers_names: List[str]
    actors: List[ActorsWriters]
    writers: List[ActorsWriters]


class Transform(object):
    elastic_format: Dict

    def __init__(self, films_linked):
        self.raw_films_linked = films_linked
        self.films_linked = [DbFilmPerson.model_validate(film_dict)
                             for film_dict in films_linked]

    def reformat(self):
        """Приводим данные ближе к формату elasticsearch"""
        step_one = {}  # type: Dict[UUID, Dict]
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


class ElasticsearchLoader(object):

    def __init__(self, transform_object: Transform):
        self.es = Elasticsearch(hosts=os.environ.get('ELASTIC_HOSTS'))
        self.ts = transform_object

    def load_it(self):
        """Загружем данные в elasticsearch."""

        actions = [
            {
                "_index": "movies",
                "_id": str(filmwork_id),
                "_source": es_film.model_dump()
            }
            for filmwork_id, es_film in self.ts.elastic_format.items()
        ]
        helpers.bulk(self.es, actions=actions)


class JsonFileStorage(object):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        json_state = json.dumps(state,
                                sort_keys=True,
                                indent=1,
                                cls=DjangoJSONEncoder  # из-за datetime
                                )
        with open(self.file_path, 'w+') as file_:
            file_.write(json_state)

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        try:
            with open(self.file_path) as file_:
                data = file_.read()  # readline
                json_data = json.loads(data)
            return json_data
        except FileNotFoundError:
            return {}


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: JsonFileStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        data = self.storage.retrieve_state()
        data[key] = value
        self.storage.save_state(data)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        data = self.storage.retrieve_state()
        try:
            result = data[key]
        except KeyError:
            result = None
        return result


def max_date(last_date: datetime.datetime,
             new_date: datetime.datetime):
    if last_date is None:
        last_date = new_date
    last_date = new_date if new_date > last_date else last_date
    return last_date


def create_elastic_index():
    """Проверяем наличие индекса, создаём при необходимости."""
    logger.info('Проверяем наличие индекса.')
    es = Elasticsearch(hosts=os.environ.get('ELASTIC_HOSTS'))
    if not es.indices.exists(index='movies'):
        logger.info('Создаём индекс.')
        with open('./create_schema/create_schema.json') as file_:
            data = file_.read()
            data = json.loads(data)
            es.indices.create(index='movies',
                              settings=data['settings'],
                              mappings=data['mappings'],)
        logger.info('Индекс создан.')


def main():
    """Основной метод запуска синхронизации.

    Запускает остальной функционал в несколько прогонов,
    для обеспечения полноты копирования и распределения нагрузки.
    """
    create_elastic_index()
    logger.info('Синхронизация модифицированных записей.')

    path = './cache/main.txt'

    n_run = 1  # номер прогона.
    # через парсер для избежания конфликта типов
    start_time = parser().parse('1970-01-01T00:00:00.000Z')
    limit_size = int(os.environ.get('LIMIT_SIZE', 100))
    last_max_modified = None

    storage = JsonFileStorage(path)
    state = State(storage)
    global_state = state.get_state('global_state')
    global_n_run = state.get_state('global_n_run')

    if global_state == CacheStates.START:
        logger.warning('Предыдущий процесс ещё не завершился')
        exit()

    cached_modified = state.get_state('modified_after')
    if cached_modified:
        modified_after = parser().parse(cached_modified)
    else:
        modified_after = start_time

    if global_state == CacheStates.ERROR:
        n_run = global_n_run

    try:
        postgres_saver = PostgresSaver()
        state.set_state('global_state', CacheStates.START)
        while True:
            state.set_state('global_state', CacheStates.START)
            state.set_state('global_n_run', n_run)
            pp = PostgresProducer(postgres_saver, limit_size,
                                  modified_after, n_run)
            pp.collect()

            if not pp.has_results:  # событие остановки
                state.set_state('global_state', CacheStates.FINISH)
                # дата с предыдущего прогона
                state.set_state('modified_after', last_max_modified)
                logger.info('Синхронизация завершена.')
                break

            last_max_modified = max_date(last_max_modified,
                                         pp.max_modified_after)

            n_run2 = 1
            while True:
                pe = PostgresEnricher(pp, limit_size, modified_after, n_run2)
                pe.collect()
                if not pe.has_results:  # событие остановки
                    last_max_modified = max_date(last_max_modified,
                                                 pe.max_modified_after)
                    break
                # собираем. собрали всё за этот прогон, пишем.
                # Вроде допущение соблюдается... хотя тут жанры...

                # если изменить 1 жанр то изменятся тысячи произведений...
                # получается мержить и заливать, мержить и заливать...
                pm = PostgresMerger(postgres_saver, modified_after,
                                    pe.results['get_person_links'],
                                    pe.results['get_genre_links'])
                pm.collect()
                last_max_modified = max_date(last_max_modified,
                                             pm.max_modified_after)

                tr = Transform(pm.results['get_films_linked'])
                tr.reformat()

                es = ElasticsearchLoader(tr)
                es.load_it()
                n_run2 += 1
            n_run += 1

    except Exception as e:
        state.set_state('global_state', CacheStates.ERROR)
        logger.info(f'Ошибка. {e}')
        raise e


if __name__ == '__main__':
    main()
