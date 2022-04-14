"""
Get data from:
https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz
"""
import gzip
import json
import jsonlines
from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from tqdm import tqdm

def preprocess_line(line):
    line = json.loads(line.decode()[:-2])
    return line

def sort_statements(entry, k=1000):
    statements = entry['statements']
    statements_keys = list(statements.keys())
    
    # check for unexpected properties
    for statements_key in statements_keys:
        if not statements_key.startswith('P'):
            print(f"strange property detected: {entry['id']} - {statements_key}")
        
    predicate_id = [int(statement[1:]) for statement in statements_keys]
    sorted_predicate_id = sorted( predicate_id )
    
    new_statements = { 
        'P'+str(statement_key): statements['P'+str(statement_key)] 
        for statement_key in sorted_predicate_id[:k] 
        if 'P'+str(statement_key) in statements_keys
    }
    entry['statements'] = new_statements
    return entry

def parse_wiki_entity(entry):
    wd_article = {}
    wd_article['id'] = entry['id']
    wd_article['label'] = entry['labels']['en']['value']
    wd_article['aliases'] = [_['value'] for _ in entry['aliases']['en']] if 'en' in entry['aliases'] else []
    wd_article['descriptions'] = entry['descriptions']['en']['value'] if 'en' in entry['descriptions'] else ''

    # get relations
    all_claims = entry['claims']
    relations_dict = {}
    for property_, claims in all_claims.items():
        # this will only fetch statements that link to another entity
        for claim in claims:
            try:
                obj_ = claim['mainsnak']['datavalue']['value']['id']
                # first time seeing this property
                if property_ not in relations_dict:
                        relations_dict[property_] = []
                relations_dict[property_].append(obj_)
            except:
                continue
                
        # remove repeats
        if property_ in relations_dict: relations_dict[property_] = list(set(relations_dict[property_]))
        
    wd_article['statements'] = relations_dict
    return wd_article

if __name__=="__main__":

    es = Elasticsearch()
    entity_indexname = 'wikidata-entities'

    # 1. Create articles index
    index_name = 'wikidata-entities'
    mapping = '''
    {  
    "mappings":{  
        "properties":{  
            "id":{  
            "type":"text"
            },
            "label":{  
            "type":"text"
            },
            "aliases":{  
            "type":"text"
            },
            "descriptions":{  
            "type":"text"
            },
            "statements":{
            "enabled": false
            }
        }
        }
    }
    '''
    es.indices.create(index=entity_indexname, body=mapping)

    # 2 load the entities
    es_ingestible_entries = []
    batchsize=100
    filename = 'data/latest-all.json.gz'

    with gzip.open(filename, 'r') as fin:
        fin.readline()

        for i in tqdm( range(94083277) ):
    #     for i in tqdm( range(1000000) ):
            # read and load into a dict
            entry = fin.readline()
            if entry.decode().endswith('\r\n'): entry = json.loads(entry.decode()[:-2])
            if entry.decode().endswith('\n'): entry = json.loads(entry.decode()[:-1]) 

            # filter out non english entries
            if 'en' not in entry['labels']:
                continue
                
            # skip properties
            if not entry['id'].startswith('Q'):
                continue

            # parse entities here
            if entry['type'] == 'item':
                parsed_entry = parse_wiki_entity(entry)
                sorted_parsed_entry = sort_statements(parsed_entry)
                es_ingestible_entry = {
                        "_index": entity_indexname,
                        "_type": "_doc",
                        "_source": parsed_entry,
                        "_op_type":'index'
                    }
                es_ingestible_entries.append(es_ingestible_entry)

            # Upload into es
            if len(es_ingestible_entries)>=batchsize:
                helpers.bulk(es, es_ingestible_entries)
                es_ingestible_entries=[]
                
        # ingest last bit of articles
        # an error may be expected at this point 
        # if the dump ends with an abrupt ']'
        helpers.bulk(es, es_ingestible_entries)