from functools import wraps
from time import sleep
from typing import Optional

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extensions import cursor as _cursor
from psycopg2.extras import RealDictCursor

from config import DbConf
from lib import get_logger

db_conf = DbConf()
logger = get_logger('etl module')


def backoff(start_sleep_time: float = 0.1,
            factor: int = 2,
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


class PostgresSaver:
    """Класс работы c postgresql, запись/чтение(для тестов)."""

    dsl_dict: dict
    connection: _connection
    cursor: _cursor

    def __init__(self, dsl_dict: Optional[dict] = None) -> None:
        """На вход - коннект к базе"""

        self.dsl_dict = dsl_dict or {
            'dbname': db_conf.name,
            'user': db_conf.user,
            'password': db_conf.password,
            'host': db_conf.host,
            'port': db_conf.port,
        }
        self.connection, self.cursor = self.connect()

    @backoff()
    def connect(self) -> tuple[_connection, _cursor]:
        """Устанавливаем соединение с базой"""
        connection = psycopg2.connect(
            **self.dsl_dict, cursor_factory=RealDictCursor,
        )
        cursor = connection.cursor()
        return connection, cursor

    def execute(self, query: str) -> list:
        """Собираем выборку с базы."""
        try:
            self.cursor.execute(query)
        except (psycopg2.Error, psycopg2.Warning) as exc:
            self.cursor.close()
            self.connection.close()
            raise exc
        raw_data = self.cursor.fetchall()
        return [dict(row) for row in raw_data]

    def disconnect(self) -> None:
        self.cursor.close()
        self.connection.commit()
        self.connection.close()

    def __del__(self) -> None:
        """Закрываем соединение, при удалении объекта."""
        try:
            if self.connection is not None:
                self.disconnect()
        except Exception as e:
            pass
