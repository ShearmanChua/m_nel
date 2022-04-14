"""
Get data from:
https://github.com/SDM-TIB/falcon2.0
https://figshare.com/articles/dataset/Falcon_2_0_background_knowledge_-_Wikidata_labels_alignments/11362883
"""
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from tqdm import tqdm

if __name__ == "__main__":
    filepath = 'data/wikidatapropertyindex.json'
    es = Elasticsearch()
    es_ingestible_entries = []
    es_ingestible_entries=100
    entity_indexname = 'wikidata-properties'

    # 1. Create articles index
    index_name = 'wikidata-properties'
    mapping = '''
    {  
    "mappings":{  
        "properties":{  
            "title":{  
            "type":"text"
            },
            "uri":{  
            "type":"text"
            }
            "id":{  
            "type":"text"
            }
        }
        }
    }'''
    es.indices.create(index=index_name, body=mapping)

    # 2. load the data
    with open(filepath, 'r') as file:
        properties_json = file.readlines()

    def add_field(line):
        doc = json.loads(line)
        doc['_source']['id'] = doc['_source']['uri'].replace('<','').replace('>','').split('/')[-1]
        doc['_index'] = 'wikidata-properties'
        doc['_type'] = '_doc'
        return doc

    added_properties_json = [add_field(line) for line in properties_json]

    helpers.bulk(es, added_properties_json)