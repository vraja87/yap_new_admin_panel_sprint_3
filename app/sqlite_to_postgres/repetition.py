import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, astuple, dataclass, fields
from pprint import pprint
from uuid import uuid4

import psycopg2


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn # С конструкцией yield вы познакомитесь в следующем модуле
    # Пока воспринимайте её как return, после которого код может продолжить выполняться дальше
    conn.close()


# Задаём путь к файлу с базой данных
# db_path = 'db.sqlite'
# conn = sqlite3.connect(db_path)
#
# conn.row_factory = sqlite3.Row  # «ключ-значение»
# curs = conn.cursor()
# # Формируем запрос. Внутри execute находится обычный SQL-запрос
# curs.execute("SELECT * FROM film_work;")
# data = curs.fetchall()
# # print(dict(data[0]))
# pprint(dict(data[0]))
# # Разрываем соединение с БД
#
# conn.close()

# db_path = 'db.sqlite'
# conn = sqlite3.connect(db_path)
# curs = conn.cursor()
# curs.execute("SELECT * FROM film_work;")
# a = curs.fetchall()
# b = curs.fetchall()
# conn.close()
# pprint(b)

# db_path = 'db.sqlite'
# with conn_context(db_path) as conn:
#     curs = conn.cursor()
#     curs.execute("SELECT * FROM film_work;")
#     data = curs.fetchall()
#     print(dict(data[0]))
# Тут соединение уже закрыто


import io

import psycopg2

dsn = {
    'dbname': 'movies_database',
    'user': 'app',
    'password': '123qwe',
    'host': 'localhost',
    'port': 5432,
    'options': '-c search_path=content',
}

with psycopg2.connect(**dsn) as conn, conn.cursor() as cursor:
    # Очищаем таблицу в БД, чтобы загружать данные в пустую таблицу
    cursor.execute("""TRUNCATE content.temp_table""")

    # Одиночный insert
    data = ('ca211dbc-a6c6-44a5-b238-39fa16bbfe6c', 'Иван Иванов')
    cursor.execute(
        """INSERT INTO content.temp_table (id, name) VALUES (%s, %s)""", data)

    # Множественный insert
    # Обращаем внимание на подготовку параметров для VALUES через cursor.mogrify
    # Это позволяет без опаски передавать параметры на вставку
    # mogrify позаботится об экранировании и подстановке нужных типов
    # Именно поэтому можно склеить тело запроса с подготовленными параметрами
    data = [
        ('b8531efb-c49d-4111-803f-725c3abc0f5e', 'Василий Васильевич'),
        ('2d5c50d0-0bb4-480c-beab-ded6d0760269', 'Пётр Петрович')
    ]
    args = ','.join(cursor.mogrify("(%s, %s)", item).decode() for item in data)
    cursor.execute(f"""
    INSERT INTO content.temp_table (id, name)
    VALUES {args}
    """)

    # Пример использования UPSERT — обновляем уже существующую запись
    data = ('ca211dbc-a6c6-44a5-b238-39fa16bbfe6c', 'Иван Петров')
    cursor.execute("""
    INSERT INTO content.temp_table (id, name)
    VALUES (%s, %s)
    ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name
    """, data)

    cursor.execute(
        """SELECT name FROM content.temp_table WHERE id = 'ca211dbc-a6c6-44a5-b238-39fa16bbfe6c'""")
    result = cursor.fetchone()
    print('Результат выполнения команды UPSERT ', result)

    # Используем команду COPY
    # Для работы COPY требуется взять данные из файла или подготовить файловый объект через io.StringIO
    cursor.execute("""TRUNCATE content.temp_table""")
    data = io.StringIO()
    data.write('ca211dbc-a6c6-44a5-b238-39fa16bbfe6c,Михаил Михайлович')
    data.seek(0)
    cursor.copy_expert(
        """COPY content.temp_table FROM STDIN (FORMAT 'csv', HEADER false)""",
        data)

    cursor.execute(
        """SELECT name FROM content.temp_table WHERE id = 'ca211dbc-a6c6-44a5-b238-39fa16bbfe6c'""")
    result = cursor.fetchone()
    print('Результат выполнения команды COPY ', result)

#
# @dataclass
# class User:
#     id: uuid4
#     name: str
#
#
# with psycopg2.connect(**dsn) as conn, conn.cursor() as cursor:
#     cursor.execute("""SELECT id, name FROM content.temp_table""")
#
#     result = cursor.fetchone()
#
#     user = User(**dict(
#         result))  # User(id='b8531efb-c49d-4111-803f-725c3abc0f5e', name='Василий Васильевич')
#     asdict(user)  # {'id': 'b8531efb-c49d-4111-803f-725c3abc0f5e', 'name': 'Василий Васильевич'}
#     astuple(user)  # ('b8531efb-c49d-4111-803f-725c3abc0f5e', 'Василий Васильевич')
#     [field.name for field in fields(user)]  # [id, name]


def save_xxx_to_postgres():
    # Получаем названия колонок таблицы (полей датакласса)
    column_names = [field.name for field in fields(user)]  # id, name

    # В зависимости от количества колонок генерируем под них %s.
    col_count = ', '.join(['%s'] * len(column_names))  # '%s, %s

    bind_values = ','.join(
        cursor.mogrify(f"({col_count})", row).decode('utf-8') for row in
        astuple(user))

    query = (
        f'INSERT INTO content.{table_name} ({column_names}) VALUES {bind_values} '
        f' ON CONFLICT (id) DO NOTHING'
        )


