"""Проверка целостности данных.

Между каждой парой таблиц в SQLite и Postgres.
"""

import os

import pytest
from dotenv import load_dotenv

from sqlite_to_postgres.load_data import (DbFilmWork, DbGenre, DbGenreFilmwork,
                                          DbPerson, DbPersonFilmwork,
                                          PostgresSaverExtractor,
                                          SQLiteExtractor, conn_context_psql,
                                          conn_context_sqlite)

load_dotenv()

dsl = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'port': os.environ.get('DB_PORT', 5432),
}
sqlite_db_path = os.environ.get('TEST_DB_SQLITE_PATH')


class TestDbConsistency(object):

    @pytest.fixture(scope='function')
    def db_sqlite(self):
        """Коннект к sqlite.

        Yields:
            SQLiteExtractor
        """
        with conn_context_sqlite(sqlite_db_path) as sqlite_conn:
            yield SQLiteExtractor(sqlite_conn)

    @pytest.fixture(scope='function')
    def db_psql(self):
        """Коннект к postgres.

        Yields:
            PostgresSaverExtractor
        """
        with conn_context_psql(dsl) as pg_conn:
            yield PostgresSaverExtractor(pg_conn)

    @pytest.mark.parametrize(
        'dbclass',
        [DbGenre, DbPerson, DbFilmWork, DbPersonFilmwork, DbGenreFilmwork],
    )
    def test_table_length(self, dbclass, db_sqlite: SQLiteExtractor,
                          db_psql: PostgresSaverExtractor):
        """Проверяем количество записей в каждой таблице."""
        table_name = dbclass.get_table_name()
        query_sqlite = 'SELECT Count(*) FROM {table_name};'.format(
            table_name=table_name,
        )
        query_psql = 'SELECT Count(*) FROM content.{table_name};'.format(
            table_name=table_name,
        )
        len_sqlite = db_sqlite.execute(query_sqlite)
        len_psql = db_psql.execute(query_psql)
        len_sqlite = list(len_sqlite[0].values())[0]
        len_psql = list(len_psql[0].values())[0]
        assert len_sqlite == len_psql

    @pytest.mark.parametrize(
        'dbclass,extract_size',
        [(DbGenre, 100), (DbPerson, 100), (DbFilmWork, 100),
         (DbPersonFilmwork, 100), (DbGenreFilmwork, 100)],
    )
    def test_consistincy(self,
                         dbclass,
                         extract_size,
                         db_sqlite: SQLiteExtractor,
                         db_psql: PostgresSaverExtractor):
        """Проверка содержимого записей внутри каждой таблицы.

        Все записи из PostgreSQL присутствуют
        с такими же значениями полей, как и в SQLite.

        Сравнение идёт группами по n-строк.

        Всё равно косвенно перепроверяется, что выборки одинакового размера,
        т.к. тесты могут запускаться независимо друг от друга
        """
        db_sqlite.max_extract_size = extract_size
        db_psql.max_extract_size = extract_size

        sqlite_generator = db_sqlite.extract_in_dataclasses(dbclass)
        psql_generator = db_psql.select_to_dataclass(dbclass)

        delta_rows = set()
        stop_sqlite = stop_psql = False
        while True:
            sqlite_set = psql_set = set()
            try:
                sqlite_set = set(next(sqlite_generator))
            except StopIteration:
                stop_sqlite = True

            try:
                psql_set = set(next(psql_generator))
            except StopIteration:
                stop_psql = True

            if stop_sqlite or stop_psql:
                assert stop_sqlite and stop_psql  # одновременная остановка
                break

            if not sqlite_set or not psql_set:
                break

            delta = sqlite_set ^ psql_set
            delta_rows |= delta

        assert not delta_rows  # быстрее чем __eq__ в случае ошибок
