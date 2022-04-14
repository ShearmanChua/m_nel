import warnings
import json
from elasticsearch import Elasticsearch

class ES_WikiData:
    def __init__(self, host='127.0.0.1', head_entity_index = "wikidata-head-entities", entity_index = "wikidata-entities", property_index = "wikidata-properties"):
        self.es = Elasticsearch(host=host, timeout=20, max_retries=10, retry_on_timeout=True)
        self.head_entity_index = head_entity_index
        self.entity_index = entity_index
        self.property_index = property_index
        
    def get_entity(self, entity_id, indexname='', verbose=0):
        """
        Example: Belgium has entity Q31
        """
        indexname = self.entity_index if indexname=='' else indexname
        res = self.es.search(index=indexname, body={"query": {"match": {'id':entity_id}}})
        
        if res['hits']['total']['value']==0:
            if verbose: warnings.warn(f"entity {entity_id} not found")
            return None
        
        doc = res['hits']['hits'][0]['_source']
        if doc['id'] == entity_id:
            return doc
        else:
            if verbose: warnings.warn(f"entity {entity_id} not found")
            return None
        
    def get_property(self, property_id, indexname='', verbose=0):
        """
        Example: HQ has entity P159
        """
        indexname = self.property_index if indexname=='' else indexname
        res = self.es.search(index=indexname, body={"query": {"match": {'id':property_id}}})
        
        if res['hits']['total']['value']==0:
            if verbose: warnings.warn(f"property {property_id} not found")
            return None
        
        doc = res['hits']['hits'][0]['_source']
        if doc['id'] == property_id:
            return doc
        else:
            if verbose: warnings.warn(f"property {property_id} not found")
            return None
    
    def get_graph_from_wikidata_id(self, entity_id, k=10):
        head_doc = self.get_entity(entity_id)
        if head_doc is None: return ''
        
        # add label and descriptions
        graph_string = ''
        graph_string += head_doc['label']
        # graph_string += ' | '+ head_doc['descriptions']
        graph_string += ' '+ head_doc['descriptions']
        
        # retrieve relevant properties and entities
        search_list = []
        for property_id, list_of_objects in list(head_doc['statements'].items())[:k]:
            search_list.append(property_id)
            search_list.extend(list_of_objects)
        if len(search_list)==0:
            # there are no statements for some of these docs
            return graph_string
        relevant_ent_prop = self.msearch(search_list)
            
        # craft context string
        for property_id, list_of_objects in list(head_doc['statements'].items())[:k]:
            if property_id not in relevant_ent_prop:
                continue
            
            # add the property names
            property_doc = relevant_ent_prop[property_id] 
            # graph_string += ' | ' + property_doc['label'] + ' '
            graph_string += ' ' + property_doc['label'] + ' '
            
            # add the relevant obj names
            for related_ent in list_of_objects:
                if related_ent not in relevant_ent_prop:
                    continue
                # graph_string += relevant_ent_prop[related_ent]['label'] + ' '
                graph_string += relevant_ent_prop[related_ent]['label'] + ' '
        graph_string = graph_string.strip() # cut of lingering empty spaces

        return graph_string

    def generate_candidates(self, label, size=10, indexname=''):
        """
        Search for entities given a label string
        """
        indexname = self.head_entity_index if indexname=='' else indexname
        res = self.es.search(index=indexname, body={"query": {"match": {'label':label}}, "size":size})
        hits = [hit['_source'] for hit in res['hits']['hits']]
        return hits, res
    
    def msearch(self, id_iterable, size=10):
        """
        Run ES multisearch query
        """
        search_arr = []
        for id_ in id_iterable:
            if id_.startswith('Q'):
                index_ = self.entity_index 
            elif id_.startswith('P'):
                index_ = self.property_index
            else:
                # raise ValueError(f"id not recognized: {id_}")
                pass
            # req_head
            search_arr.append({'index': index_, 'type': '_doc'})
            # req_body
            search_arr.append({"query": {"match" : {"id" : id_}}, 'size': size})

        request = ''
        for each in search_arr:
            request += '%s \n' %json.dumps(each)

        # as you can see, you just need to feed the <body> parameter,
        # and don't need to specify the <index> and <doc_type> as usual 
        res = self.es.msearch(body = request)
        res = res['responses']
        res = [ one_res['hits']['hits'][0]['_source'] for one_res in res if len(one_res['hits']['hits'])!=0 ]
        res = { one_res['id']:one_res for one_res in res}
        return res