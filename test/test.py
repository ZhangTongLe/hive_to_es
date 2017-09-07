from elasticsearch import Elasticsearch

es = Elasticsearch(hosts=["58.246.241.202:9200"],
                   http_auth=("elastic", "888888"))

es.delete(index="stat", doc_type="user_action_per_day", id="AV5Z7FoT1ooCllQiPIif")
