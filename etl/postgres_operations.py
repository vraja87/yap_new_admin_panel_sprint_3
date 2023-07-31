import abc
from datetime import datetime
from functools import wraps
from typing import Optional

from dateutil.parser import parser

from config import CacheConf
from lib import CacheStates, JsonFileStorage, State
from postgres_saver import PostgresSaver

cache_conf = CacheConf()


def write_operations_state():
    """Декоратор сохранения состояние метода класса и его результата.

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


class PostgresMixin(abc.ABC):
    """Общий код классов работы с базой."""
    max_modified_after: Optional[datetime]
    modified_after: Optional[datetime]
    has_results: bool
    path: str
    results: dict

    def __init__(self, modified_after: datetime) -> None:
        self.storage = JsonFileStorage(self.path)
        self.state = State(self.storage)
        self.results = {}
        self.has_results = False

        self.modified_after = self.max_modified_after = modified_after

    @staticmethod
    def get_max_modified(ready_result: dict) -> datetime:
        """Возвращает максимальное время для поля modified"""
        a = []
        for res in ready_result:
            if isinstance(res['modified'], datetime):
                a.append(res['modified'])
            else:
                a.append(parser().parse(res['modified']))
        return max(a)

    def analyze_result(self, result: dict) -> None:
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
    def _collect_methods(self) -> tuple:
        """Возвращает кортеж методов для выполнения collect"""

    def collect(self) -> None:
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
    modified_after: datetime
    max_modified_after: Optional[datetime]
    n_run: int

    path: str = cache_conf.producer

    def __init__(self,
                 postgres_saver: PostgresSaver,
                 limit_size: int,
                 modified_after: datetime,
                 n_run: int) -> None:
        super().__init__(modified_after)
        self.postgres_saver = postgres_saver
        self.limit_size = limit_size

        self.n_run = n_run
        self.offset_size = limit_size * (n_run - 1)

    @write_operations_state()
    def get_person(self) -> list:
        query = f"""
            SELECT id, modified
            FROM content.person
            WHERE modified > '{self.modified_after}'
            ORDER BY modified
            LIMIT {self.limit_size} OFFSET {self.offset_size};"""
        result = self.postgres_saver.execute(query)
        return result

    @write_operations_state()
    def get_genre(self) -> list:
        query = f"""
            SELECT id, modified
            FROM content.genre
            WHERE modified > '{self.modified_after}'
            ORDER BY modified
            LIMIT {self.limit_size} OFFSET {self.offset_size};"""
        result = self.postgres_saver.execute(query)
        return result

    @write_operations_state()
    def get_filmwork(self) -> list:
        query = f"""
            SELECT id, modified
            FROM content.film_work
            WHERE modified > '{self.modified_after}'
            ORDER BY modified
            LIMIT {self.limit_size} OFFSET {self.offset_size};"""
        result = self.postgres_saver.execute(query)
        return result

    def _collect_methods(self) -> tuple:
        return self.get_person, self.get_genre, self.get_filmwork


class PostgresEnricher(PostgresMixin):
    """Дополняет инфу по фильмам, инфой о актёрах и жанрах"""
    limit_size: int
    path: str = cache_conf.enricher

    def __init__(self,
                 ready_producer: PostgresProducer,
                 limit_size: int,
                 modified_after: datetime,
                 n: int) -> None:
        super().__init__(modified_after)
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
    def get_person_links(self) -> list:
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
    def get_genre_links(self) -> list:
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

    def _collect_methods(self) -> tuple:
        return self.get_person_links, self.get_genre_links


class PostgresMerger(PostgresMixin):

    path: str = cache_conf.merger

    def __init__(self,
                 postgres_saver: PostgresSaver,
                 modified_after: datetime,
                 genre_results: dict,
                 person_results: dict):
        super().__init__(modified_after)
        self.postgres_saver = postgres_saver
        self.genre_results = genre_results
        self.person_results = person_results

    @write_operations_state()
    def get_films_linked(self) -> list:
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

    def _collect_methods(self) -> tuple:
        return self.get_films_linked,
