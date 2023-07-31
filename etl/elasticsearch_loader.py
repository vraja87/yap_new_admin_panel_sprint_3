from elasticsearch import Elasticsearch, helpers

from config import ElasticConf
from transform import Transform

elastic_conf = ElasticConf()


class ElasticsearchLoader:
    es: Elasticsearch
    ts: Transform

    def __init__(self, transform_object: Transform) -> None:
        self.es = Elasticsearch(hosts=elastic_conf.hosts)
        self.ts = transform_object

    def load_it(self) -> None:
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
