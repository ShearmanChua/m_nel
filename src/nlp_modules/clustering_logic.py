from datetime import datetime
from elasticsearch import Elasticsearch
from .abstract_classes import Clustering_Logic

class Last_Day_Articles(Clustering_Logic):
    
    def __init__(self, host='127.0.0.1'):
        self.es = Elasticsearch(host = host, timeout=20, max_retries=10, retry_on_timeout=True)
        
    def retrieve_articles(self):
        res = self.es.search(index="marvis-articles", body={
                          "query": {
                            "range": {
                              "date_download": {
                                "gte": "now-1d/d",
                                "lte": "now/d"
                              }
                            }
                          }
                        }
                 )
        articles = res['hits']['hits'] if res['hits']['total']['value'] != 0 else []
        articles = [article['_source'] for article in articles]
        return articles

class Unclustered_Articles(Clustering_Logic):
    
    def __init__(self, host='127.0.0.1'):
        self.es = Elasticsearch(host = host, timeout=20, max_retries=10, retry_on_timeout=True)
        
    def get_latest_timestamp(self):
        body = {
              "query": {
                "match_all": {}
              },
              "size": 1,
              "sort": [
                {
                  "datetime_clustered": {
                    "order": "desc"
                  }
                }
              ]
            }
        res = self.es.search(index="marvis-clusters", body=body)
        if res['hits']['total']['value']==0: 
            latest_timestamp = str(datetime(year=1970, month=1, day=1).date())
        else:
            print( res['hits']['hits'][0]['_source'])
            latest_timestamp = res['hits']['hits'][0]['_source']['datetime_generated']
        return latest_timestamp
        
    def retrieve_articles(self):
        latest_timestamp = self.get_latest_timestamp()
        # print(latest_timestamp)
        # print(type(latest_timestamp))
        res = self.es.search(index="marvis-articles", body={
                          "from" : 0, "size" : 500,
                          "query": {
                            "range": {
                              "date_download": {
                                "gt": latest_timestamp
                              }
                            }
                          }
                        }
                 )
        articles = res['hits']['hits'] if res['hits']['total']['value'] != 0 else []
        articles = [article['_source'] for article in articles]
        return articles