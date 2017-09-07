from elasticsearch import Elasticsearch
from elasticsearch import helpers as elasticsearch_helper


class ESClient:
    def __init__(self, hosts, http_auth=None):
        self.es = Elasticsearch(hosts=hosts, http_auth=http_auth)

    def bulk(self, actions):
        if len(actions) > 0:
            elasticsearch_helper.bulk(self.es, actions)

