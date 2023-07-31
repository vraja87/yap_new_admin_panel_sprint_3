from pydantic_settings import BaseSettings, SettingsConfigDict

env_file = '.env'


class DbConf(BaseSettings):
    model_config = SettingsConfigDict(env_file=env_file, env_prefix='DB_')

    name: str
    user: str
    password: str
    host: str
    port: str


class ElasticConf(BaseSettings):
    model_config = SettingsConfigDict(env_file=env_file, env_prefix='ELASTIC_')

    hosts: str


class CacheConf(BaseSettings):
    model_config = SettingsConfigDict(env_file=env_file, env_prefix='CACHE_')

    main: str = './cache/main.txt'
    producer: str = './cache/postgres_producer.txt'
    enricher: str = './cache/postgres_enricher.txt'
    merger: str = './cache/postgres_merger.txt'


class LogConf(BaseSettings):
    model_config = SettingsConfigDict(env_file=env_file, env_prefix='LOG_')

    etl: str = './log/etl.log'


class MainConf(BaseSettings):
    model_config = SettingsConfigDict(env_file=env_file, env_prefix='MAIN_')

    limit_size: int = 100  #
    sleep_period: int = 60  # период ожидания после выполнения скрипта
