import json
import logging
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

from config import LogConf

log_conf = LogConf()


class CacheStates:
    FINISH = 'finish'
    START = 'start'
    ERROR = 'error'


class JsonFileStorage:
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        json_state = json.dumps(state,
                                sort_keys=True,
                                indent=1,
                                cls=DjangoJSONEncoder  # из-за datetime
                                )
        with open(self.file_path, 'w+') as file_:
            file_.write(json_state)

    def retrieve_state(self) -> dict[str, Any]:
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


def get_logger(logger_name: str) -> logging.Logger:
    format_ = '%(asctime)s %(levelname)s ' \
             'PID[%(process)d] %(module)s: %(message)s'
    logger_ = logging.getLogger(logger_name)
    logger_.setLevel(logging.INFO)
    if not logger_.handlers:
        fh = logging.FileHandler(log_conf.etl)
        formatter = logging.Formatter(format_)
        fh.setFormatter(formatter)
        logger_.addHandler(fh)
    return logger_
