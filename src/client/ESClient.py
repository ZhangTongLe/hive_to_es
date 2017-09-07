from elasticsearch import Elasticsearch
from elasticsearch import helpers as elasticsearch_helper


class ESClient:
    def __init__(self, hosts, http_auth=None):
        self.es = Elasticsearch(hosts=hosts, http_auth=http_auth)

    def bulk(self, actions):
        if len(actions) > 0:
            elasticsearch_helper.bulk(self.es, actions)

    def insert(self, index, doc_type, body, doc_id=None):
        obj_list = [('_index', index), ('_type', doc_type), ('_source', body)]
        if doc_id is not None:
            obj_list.append(('_id', doc_id))
        obj = dict(obj_list)
        elasticsearch_helper.bulk(self.es, [obj])

    def query(self, query_body):
        pass
