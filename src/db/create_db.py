import argparse
from elasticsearch import Elasticsearch

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default='127.0.0.1', help="elasticsearch host")
    args = parser.parse_args()
        
    es = Elasticsearch(args.host)

    # 1. Create articles index
    index_name = 'marvis-articles'
    mapping = '''
    {  
      "mappings":{  
          "properties":{  
            "title":{  
              "type":"text"
            },
            "maintext":{  
              "type":"text"
            },
            "description":{  
              "type":"text"
            },
            "url":{  
              "type":"text"
            },
            "date_download":{
              "type":"date"
            },
            "date_publish":{
              "type":"date"
            },
            "_embedding":{
              "type":"dense_vector",
              "dims":512
            },
            "locations":{
              "type":"nested"
            },
            "entities":{
              "type":"nested"
            },
            "domain":{  
              "type":"text"
            }
          }
        }
    }'''
    res = es.indices.create(index=index_name, body=mapping)
    print(f"\n{res}")

    # 2. create clusters db
    # https://stackoverflow.com/questions/20723379/return-the-most-recent-record-from-elasticsearch-index
    index_name = 'marvis-clusters'
    mapping = '''
    {  
      "mappings":{  
          "properties":{  
            "article-ids":{  
              "type":"text"
            },
            "datetime_clustered":{
              "type":"date",
              "format": "yyyy/MM/dd HH:mm:ss"
            },
            "cluster_summary":{  
              "type":"text"
            },
            "cluster_locations":{  
              "type":"nested"
            },
            "cluster_entities":{  
              "type":"nested"
            },
            "cluster_domains":{
              "type":"text"
            }
          }
        }
    }'''
    res = es.indices.create(index=index_name, body=mapping)
    print(f"\n{res}")