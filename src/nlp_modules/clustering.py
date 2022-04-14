from datetime import datetime
import numpy as np
from scipy import sparse
from sklearn.metrics.pairwise import cosine_similarity
import tensorflow as tf
import spacy
import markov_clustering as mc
import seaborn as sns
from tqdm import tqdm
from sklearn.cluster import DBSCAN
from loguru import logger

from .abstract_classes import Pipe
from ..utils import row2str
from .encoders.use import USE

class MKV(Pipe):

    def __init__(self, encoder=None, threshold=0.6):
        if encoder is None:
            self.encoder = USE()
        self.threshold = threshold
        self.nlp = spacy.load('resources/spacy/en_core_web_sm')

    def process(self, df, content_header='generated_abstract'):
        clustered_df, _, _ = self.cluster(df, content_header=content_header)
        return clustered_df

    def cluster(self, df, content_header='generated_abstract', chart_sim=False, chart_graph=False):
        
        # encode the texts
        texts = [row2str(row, nlp = self.nlp, content_header=content_header) for _, row in df.iterrows()]
        encodings = []
        for text in tqdm(texts, desc=f'1/3 - {datetime.now()}: encoding texts'):
            try:
                encoding = self.encoder.encode([text])
            except:
                print(text)
                raise
            encodings.append(encoding)
        encodings = tf.concat(encodings, 0)
        logger.info(encodings.shape)
        
        logger.info(f"2/3: calculating similarity matrix")
        similarity_matrix = cosine_similarity(encodings, encodings)
        if chart_sim: sns.heatmap(similarity_matrix, annot=True)
        
        logger.info(f"3/3: applying markov clustering to retrieve adjacency")
        adjacency_matrix = self.get_adjacency(similarity_matrix, self.threshold)
        clusters = self.cluster_adjacency(adjacency_matrix, chart_graph)
        df_ = add_cluster_col(df, clusters) # add cluster to df

        return df_, clusters, similarity_matrix

    def get_adjacency(self, similarity_matrix, threshold):
        """
        :return adjacency_matrix: a scipy sparse matrix indicating which nodes are connected to which
        """
        adjacency_matrix = np.where(similarity_matrix > threshold, 1, 0)
        adjacency_matrix = adjacency_matrix - np.eye(adjacency_matrix.shape[0])
        adjacency_matrix = sparse.csr_matrix(adjacency_matrix)
        return adjacency_matrix

    def cluster_adjacency(self, adjacency_matrix, verbose=False):
        """
        :return clusters: list of tuples containing the indices of articles clustered together. e.g. [(0, 1, 2, 3, 4, 5, 6), (7,), (8,), (9,), (10,), (11,), (12,), (13,)]
        """
        logger.info(f"received adjacency matrix of shape: {adjacency_matrix.shape}")
        result = mc.run_mcl(adjacency_matrix)      
        clusters = mc.get_clusters(result)  
        if verbose: mc.draw_graph(adjacency_matrix, clusters, with_labels=False, edge_color="silver")
        return clusters

class DB(Pipe):

    def __init__(self, encoder=None, threshold=0.25):
        if encoder is None:
            self.encoder = USE()
        self.nlp = spacy.load('resources/spacy/en_core_web_sm')
        self.clus = DBSCAN(eps=threshold, min_samples=2, metric='cosine')

    def process(self, df, content_header='generated_abstract'):
        clustered_df, _, _ = self.cluster(df, content_header=content_header)
        return clustered_df

    def cluster(self, df, content_header='maintext', chart_sim=False, chart_graph=False):
        
        # encode the texts
        texts = [row2str(row, nlp = self.nlp, content_header=content_header) for _, row in df.iterrows()]
        encodings = []
        for text in tqdm(texts, desc=f'1/3 - {datetime.now()}: encoding texts'):
            try:
                encoding = self.encoder.encode([text])
            except:
                print(text)
                raise
            encodings.append(encoding)
        encodings = tf.concat(encodings, 0)
        logger.info(encodings.shape)
        
        logger.info(f"2/3: calculating similarity matrix")
        similarity_matrix = cosine_similarity(encodings, encodings)
        if chart_sim: sns.heatmap(similarity_matrix, annot=True)
        
        logger.info(f"3/3: applying DBSCAN")
        clusters = self.clus.fit_predict(encodings)
        df_ = df.assign(cluster_idx = clusters) # add cluster to df

        return df_, clusters, similarity_matrix

def add_cluster_col(df, clusters):
    """
    :param clusters: list of tuples signifying the rows to their clusters
    """
    df_ = df.copy().assign(cluster_idx = np.NaN)
    for cluster_idx, cluster in enumerate( clusters ):
        df_.iloc[list(cluster), -1] = int(cluster_idx)
        
    assert df_.cluster_idx.isnull().sum()==0, "not all articles are assigned a cluster"
    return df_

