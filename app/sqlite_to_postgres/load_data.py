"""Модуль миграции данных кинопроизведений из базы sqlite в postgresql."""

import sqlite3
from contextlib import contextmanager
from dataclasses import astuple, dataclass, fields
from datetime import datetime
from os import environ
from typing import Dict, List, Union, Generator
from uuid import uuid4

import psycopg2
from dateutil.parser import parser
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import RealDictCursor

load_dotenv()


class DbDeltaNameMixin(object):
    """Миксин датаклассов ниже, c общими методами."""

    @classmethod
    def get_sqlite_column_alias(cls):
        """алиас для поля даты создания строки sqlite3/psql.

        Returns:
            Dict
        """
        return {'created': 'created_at'}

    @classmethod
    def get_psql_column_alias(cls):
        """обратный алиас, для удобства.

        Returns:
            Dict
        """
        return {
            sqlite_col: psql_col
            for psql_col, sqlite_col in
            cls.get_sqlite_column_alias().items()
        }

    @classmethod
    def get_table_name(cls) -> str:
        """Возвращает константу - имя таблицы.

        Returns:
            str
        """
        alias_dataclass_table_names = {
            DbFilmWork: 'film_work',
            DbGenre: 'genre',
            DbGenreFilmwork: 'genre_film_work',
            DbPerson: 'person',
            DbPersonFilmwork: 'person_film_work',
        }
        return alias_dataclass_table_names[cls]

    def __post_init__(self):
        """Преобразуем строковые created/modified в datetime.datetime."""
        if hasattr(self, 'created') and type(self.created) == str:
            self.created = parser().parse(self.created)

        if hasattr(self, 'modified') and type(self.modified) == str:
            self.modified = parser().parse(self.modified)


class DbDeltaNamesMixin(DbDeltaNameMixin):
    """Доп. миксин для датаклассов таблиц с полем modified."""

    @classmethod
    def get_sqlite_column_alias(cls):
        """обратный алиас, для удобства.

        Returns:
            Dict
        """
        names = DbDeltaNameMixin.get_sqlite_column_alias()
        names['modified'] = 'updated_at'
        return names


@dataclass(unsafe_hash=True)
class DbFilmWork(DbDeltaNamesMixin):
    """Датакласс таблицы content.film_work."""

    id: uuid4  # uuid
    title: str  # text
    description: str  # text
    file_path: str  # text
    creation_date: str  # date
    rating: float  # double precision
    type: str  # text
    created: str  # timestamp with time zone
    modified: str  # timestamp with time zone


@dataclass(unsafe_hash=True)
class DbGenre(DbDeltaNamesMixin):
    """Датакласс таблицы content.genre."""

    id: uuid4  # uuid
    name: str  # character varying
    description: str  # text
    created: Union[str, datetime]  # timestamp with time zone
    modified: Union[str, datetime]  # timestamp with time zone


@dataclass(unsafe_hash=True)
class DbGenreFilmwork(DbDeltaNameMixin):
    """Датакласс таблицы content.genre_film_work."""

    id: uuid4  # uuid
    genre_id: uuid4  # uuid
    film_work_id: uuid4  # uuid
    created: str  # timestamp with time zone


@dataclass(unsafe_hash=True)
class DbPerson(DbDeltaNamesMixin):
    """Датакласс таблицы content.person."""

    id: uuid4  # uuid
    full_name: str  # text
    created: str  # timestamp with time zone
    modified: str  # timestamp with time zone


@dataclass(unsafe_hash=True)
class DbPersonFilmwork(DbDeltaNameMixin):
    """Датакласс таблицы content.person_film_work."""

    id: uuid4  # uuid
    person_id: uuid4  # uuid
    film_work_id: uuid4  # uuid
    role: str  # text
    created: str  # timestamp with time zone


class PostgresSaverExtractor(object):
    """Класс работы c postgresql, запись/чтение(для тестов)."""

    max_insert_rows: int
    max_extract_size: int

    def __init__(self, psql_conn, max_insert_rows=None, max_extract_size=1000):
        """На вход - коннект к базе,максимальное количество строк на запрос.

        Args:
            psql_conn: psycopg2.connection
            max_insert_rows: Optional[int]
        """
        self.max_insert_rows = max_insert_rows
        self.max_extract_size = max_extract_size
        self.connection: psycopg2.connection = psql_conn
        self.cursor: psycopg2.cursor = self.connection.cursor()

    def insert_update(self, query: str):
        """Вставка/апдейт строк в базе.

        Args:
            query: str

        """
        try:
            self.cursor.execute(query)
        except (psycopg2.Error, psycopg2.Warning) as exc:
            self.cursor.close()
            self.connection.close()
            raise exc
        self.connection.commit()

    def execute(self, query: str) -> List:
        """Собираем выборку с базы.

        Args:
            query:  str

        Returns:
            List
        """
        try:
            self.cursor.execute(query)
        except (psycopg2.Error, psycopg2.Warning) as exc:
            self.cursor.close()
            self.connection.close()
            raise exc
        raw_data = self.cursor.fetchall()  # список
        return [dict(row) for row in raw_data]  # типа-словарь

    def execute_generator(self, query: str):
        """Собираем выборку ограниченного размера.

        Args:
            query:  str

        Yields:
            Generator
        """
        try:
            self.cursor.execute(query)
            while True:
                result_chunks = self.cursor.fetchmany(self.max_extract_size)
                if not result_chunks:
                    break
                yield result_chunks
        except (psycopg2.Error, psycopg2.Warning) as exc:
            self.cursor.close()
            self.connection.close()
            raise exc

    def select_to_dataclass(self, dt_class):
        """Генератор выборки с преобразованием результата в датаклассы.

        Args:
            dt_class: - определение датакласса

        Yields:
            Generator
        """
        column_names = [field.name for field in fields(dt_class)]
        fields_row = ', '.join(['{}'] * len(column_names))
        fields_row = fields_row.format(*column_names)
        table_name = dt_class.get_table_name()
        query = 'SELECT {fields_row} FROM content.{table_name};'.format(
            fields_row=fields_row,
            table_name=table_name)
        generator = self.execute_generator(query)
        for response in generator:
            yield [dt_class(**dict(one_row)) for one_row in response]

    def save_all_data(self, generator: Generator):
        """Обёртка для обработки генераторов"""
        for response_chunk in generator:
            self.save_data_list(response_chunk)

    def save_data_list(self, dataclasses: List):
        """Пишем все данные датаклассов кусками по n-записей.

        Args:
            dataclasses: - список датаклассов, всех поддерживаемых типов.

        Raises:
            ValueError: - в случае передачи более 1 типа датакласса
        """
        all_types = {type(data_obj) for data_obj in dataclasses}
        if len(all_types) != 1:
            raise ValueError(
                'Поддерживается обработка одного типа датакласса за раз',
            )
        one_obj = all_types.pop()
        table_name = one_obj.get_table_name()

        col_count, column_names = self._make_column_str(one_obj)

        if self.max_insert_rows:
            num_rows = self.max_insert_rows
            data_slices = [
                dataclasses[position:position + num_rows] for position in
                range(0, len(dataclasses), num_rows)
            ]
        else:
            data_slices = [dataclasses]

        for data_slice in data_slices:
            bind_values = ','.join(
                self.cursor.mogrify(
                    f"({col_count})", astuple(data_obj)).decode('utf-8')
                for data_obj in data_slice)

            query = "INSERT INTO {table_name} ({column_names}) " \
                    "VALUES {bind_values} " \
                    "ON CONFLICT (id) DO NOTHING" \
                    ";".format(
                        table_name='content.{0}'.format(table_name),
                        column_names=column_names,
                        bind_values=bind_values,)
            self.insert_update(query)

    @staticmethod
    def _make_column_str(one_obj):
        """Формируем подстроку c именами столбцов.

        Args:
            one_obj: - датакласс

        Returns:
            List[List]
        """
        column_names = [field.name for field in fields(one_obj)]
        col_count = []
        for _ in column_names:
            col_count.append('%s')
        col_count = ', '.join(col_count)
        column_names = ', '.join(column_names)
        return col_count, column_names

    def _slicer(self, dataclasses):
        """Пилим на списки фиксированной длинны.

        Args:
            dataclasses: - список датаклассов

        Returns:
            List[List]
        """
        if self.max_insert_rows:
            num_rows = self.max_insert_rows
            data_slices = [
                dataclasses[position:position + num_rows] for position in
                range(0, len(dataclasses), num_rows)
            ]
        else:
            data_slices = [dataclasses]
        return data_slices


class SQLiteExtractor(object):
    """Сборы с базы sqlite."""

    def __init__(self, sqlite_connect, max_extract_size=1000):
        """На вход - коннект к базе.

        Args:
            sqlite_connect: sqlite3.Connection
        """
        self.max_extract_size = max_extract_size
        self.connection: sqlite3.Connection = sqlite_connect
        self.cursor: sqlite3.Cursor = self.connection.cursor()

    def execute(self, query: str):
        """Собираем выборку с базы.

        Args:
            query: str

        Returns:
            List
        """
        try:
            self.cursor.execute(query)
        except (sqlite3.Error, sqlite3.Warning) as exc:
            self.cursor.close()
            self.connection.close()
            raise exc
        raw_data = self.cursor.fetchall()
        return list(map(dict, raw_data))

    def execute_generator(self, query):
        """Собираем выборку ограниченного размера.

        Args:
            query:  str

        Yields:
            Generator
        """
        try:
            self.cursor.execute(query)
            while True:
                result_chunks = self.cursor.fetchmany(self.max_extract_size)
                if not result_chunks:
                    break
                yield result_chunks
        except (sqlite3.Error, sqlite3.Warning) as exc:
            self.cursor.close()
            self.connection.close()
            raise exc

    def extract_in_dataclasses(self, dt_class):
        """Возвращает датаклассы с выборкой всей ассоциированной таблицы.

        Args:
            dt_class: - определение датакласса

        Yields:
            Generator - Генератор списков датаклассов
        """
        column_names = self._get_sqlite_column_names(dt_class)
        fields_row = []
        for col_name in column_names:
            fields_row.append('{0}'.format(col_name))
        fields_row = ', '.join(fields_row)
        query = ('SELECT {fields_row} '
                 + 'FROM {table_name};').format(
            fields_row=fields_row,
            table_name=dt_class.get_table_name(),
        )
        generator = self.execute_generator(query)
        for response in generator:
            response = self._invert_result_fields(dt_class, map(dict, response))
            yield [dt_class(**dict(one_row)) for one_row in response]

    @classmethod
    def _get_sqlite_column_names(cls, dt_class):
        delta_names = dt_class.get_sqlite_column_alias()
        return [
            field.name if field.name not in delta_names
            else delta_names[field.name] for field in fields(dt_class)
        ]

    @classmethod
    def _invert_result_fields(cls, dataclass_, sql_response: List[Dict]):
        names_inverted = dataclass_.get_psql_column_alias()
        fixed_response = []
        for one_row in sql_response:
            fixed_row = {}
            for col_name, col_value in one_row.items():
                if col_name in names_inverted:
                    col_name = names_inverted[col_name]
                fixed_row[col_name] = col_value
            fixed_response.append(fixed_row)
        return fixed_response


@contextmanager
def conn_context_sqlite(db_path):
    """Менеджер контекста подключения к базе sqlite.

    Args:
        db_path: str

    Yields:
        Iterator[`Connection`]
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # ключ-значение
    yield conn
    conn.close()


@contextmanager
def conn_context_psql(dsl_dict):
    """Менеджер контекста подключения к базе postgresql.

    Args:
        dsl_dict: Dict

    Yields:
        Iterator[`Connection`]
    """
    # здесь RealDictCursor вместо DictCursor для корректной работы fetchmany()
    connection = psycopg2.connect(
        **dsl_dict, cursor_factory=RealDictCursor,
    )
    yield connection
    connection.close()


def load_from_sqlite(sqlite_conn_: sqlite3.Connection, pg_conn_: _connection):
    """Основной метод загрузки данных из SQLite в Postgres.

    Args:
        sqlite_conn_: sqlite3.Connection
        pg_conn_: _connection
    """
    sqlite_extractor = SQLiteExtractor(sqlite_conn_)
    sqlite_data = sqlite_extractor.extract_in_dataclasses(DbFilmWork)

    max_rows_count = 15  # ограничиваем пачку до n-записей
    postgres_saver = PostgresSaverExtractor(pg_conn_, max_rows_count)
    postgres_saver.save_all_data(sqlite_data)

    sqlite_data = sqlite_extractor.extract_in_dataclasses(DbGenre)
    postgres_saver.save_all_data(sqlite_data)
    sqlite_data = sqlite_extractor.extract_in_dataclasses(DbGenreFilmwork)
    postgres_saver.save_all_data(sqlite_data)
    sqlite_data = sqlite_extractor.extract_in_dataclasses(DbPerson)
    postgres_saver.save_all_data(sqlite_data)
    sqlite_data = sqlite_extractor.extract_in_dataclasses(DbPersonFilmwork)
    postgres_saver.save_all_data(sqlite_data)


if __name__ == '__main__':
    dsl = {
        'dbname': environ.get('DB_NAME'),
        'user': environ.get('DB_USER'),
        'password': environ.get('DB_PASSWORD'),
        'host': environ.get('DB_HOST'),
        'port': environ.get('DB_PORT'),
    }
    with conn_context_sqlite(environ.get('DB_SQLITE_PATH')) as sqlite_conn:
        with conn_context_psql(dsl) as pg_conn:  # разделил из-за flake8
            load_from_sqlite(sqlite_conn, pg_conn)
