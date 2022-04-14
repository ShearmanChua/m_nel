import warnings
from datetime import datetime
import pandas as pd
from elasticsearch import helpers

es_datetime_format = lambda x: x.strftime('%Y/%m/%d %H:%M:%S')

"""
1. Func for processing articles
https://towardsdatascience.com/exporting-pandas-data-to-elasticsearch-724aa4dd8f62
"""
def safe_df(df):
    """
    Format dataframe to be safely ingested into es
    """
    def safe_date(date_value):
        return (
            pd.to_datetime(date_value) if not pd.isna(date_value)
                else  datetime.now()
        )
    def safe_value(field_val):
        return field_val if not pd.isna(field_val) else "null"

    # format dates
    df.date_download = df.date_download.apply(safe_date)
    df.date_publish = df.date_publish.apply(safe_date)
    
    # format strings
    df.title = df.title.apply(safe_value)
    df.maintext = df.maintext.apply(safe_value)
    df.description = df.description.apply(safe_value)
    df.url = df.url.apply(safe_value)
    return df

def row_article_to_doc(row, encoder):
    # get encoding
    encoding_as_list_array = encoder.encode([row.generated_abstract]).numpy().tolist()[0]

    char_split = "%20"
    # parse location tags
    if len( row.location_tag )>0:
        list_of_locations = [{"surface":location_tag_.split(char_split)[0], "qid":location_tag_.split(char_split)[1], "ent_label":location_tag_.split(char_split)[2]} for location_tag_ in row.location_tag.split(' | ') if len(location_tag_.split(char_split)[1])>0 ]
    else:
        list_of_locations = []

    # parse entity tags
    if len( row.entity_tag )>0:
        list_of_entities = [{"surface":ent_tag_.split(char_split)[0], "qid":ent_tag_.split(char_split)[1], "ent_label":ent_tag_.split(char_split)[2]} for ent_tag_ in row.entity_tag.split(' | ') if len(ent_tag_.split(char_split)[1])>0 ]
    else:
        list_of_entities = []

    # format into json docs
    json_doc = {
        "title": row.title,
        "maintext": row.maintext,
        'generated_abstract': row.generated_abstract,
        "description": row.description,
        "url": row.url,
        "date_download": row.date_download,
        "date_publish": row.date_publish,
        "_embedding": encoding_as_list_array,
        "locations": list_of_locations,
        "entities": list_of_entities,
        "domain": row.domain_tag
    }    
    return json_doc
    
def doc_generator(df, encoder, indexname='marvis-articles'):
    """
    Return generator to yield docs into es
    """
    df_iter = df.iterrows()
    for _, row in df_iter:
        yield {
                "_index": indexname,
                "_source": row_article_to_doc(row, encoder),
                "_op_type":'index'
            }
    raise StopIteration
    
def upload_article_df(es_client, df, encoder, indexname='marvis-articles'):
    df = safe_df(df)
    helpers.bulk(es_client, doc_generator(df, encoder, indexname))


"""
2. Func for processing clusters
"""
def flatten_linked_entities(list_of_list_of_ent, char_split="%20"):
    flattened_list = []
    for list_of_ent in list_of_list_of_ent:
        for ent in list_of_ent.split('|'):
            try:
                reformatted_ent = {"surface":ent.split(char_split)[0], "qid":ent.split(char_split)[1], "ent_label":ent.split(char_split)[2]}
            except Exception as e:
                warnings.warn(f"unable to parse entity: {ent} \n Check that it is separated by '%20' and '|'")
            flattened_list.append(reformatted_ent)
    return flattened_list

def get_cluster_docs(df, from_es=False):
    """
    Parse the dataframe into a list of json decodable dicts to pass into ES
    """
    cluster_generation_timestamp = es_datetime_format(datetime.now())
    clusters_to_ingest = []
    for cluster_idx in df.cluster_idx.unique():
        if cluster_idx==-1:
            continue
        one_cluster = df.loc[df.cluster_idx==cluster_idx]

        # url, locations, summary, domain
        cluster_article_urls = one_cluster.url.tolist()
        cluster_locations = [location for list_of_locations in one_cluster.location_tag.to_list() for location in list_of_locations ] if from_es else flatten_linked_entities( one_cluster.location_tag.to_list() )  
        # cluster_entities = flatten_linked_entities( one_cluster.entity_tag.to_list() ) 
        cluster_summary = one_cluster.cluster_summary.iloc[0]
        cluster_domains = ' '.join(one_cluster.domain_tag.tolist()).strip() if from_es else ' '.join(one_cluster.domain_tag.tolist()).strip()

        # doc dictionary
        clusters_to_ingest.append({
            "urls": cluster_article_urls,
            "datetime_clustered": cluster_generation_timestamp,
            "cluster_summary":cluster_summary,
            "cluster_locations": cluster_locations,
            "cluster_domains": cluster_domains
        })
    return clusters_to_ingest

def cluster_generator(clusters_to_ingest, indexname='marvis-clusters'):
    """
    Return generator to yield docs into es
    """
    for cluster in clusters_to_ingest:
        yield {
                "_index": indexname,
                "_type": "_doc",
                "_source": cluster,
                "_op_type":'index'
            }
    raise StopIteration
    
def upload_cluster_df(es_client, df, from_es=False, indexname="marvis-clusters"):
    clusters_to_ingest = get_cluster_docs(df, from_es=from_es)
    print(helpers.bulk(es_client, cluster_generator(clusters_to_ingest, indexname)))