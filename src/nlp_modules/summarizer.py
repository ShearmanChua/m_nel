from transformers import PegasusForConditionalGeneration, PegasusTokenizer
from tqdm import tqdm
import numpy as np
from .abstract_classes import Pipe

class Pegasus(Pipe):

    def __init__(self):
        # init
        self.device = 'cpu' 
        # self.device = 'cuda' 
        # self.sum_tokenizer = PegasusTokenizer.from_pretrained('google/pegasus-cnn_dailymail')
        # self.sum_model = PegasusForConditionalGeneration.from_pretrained('google/pegasus-cnn_dailymail').to(self.device)
        self.sum_tokenizer = PegasusTokenizer.from_pretrained('resources/pegasus/tokenizer')
        self.sum_model = PegasusForConditionalGeneration.from_pretrained('resources/pegasus/model').to(self.device)

    def summarize(self, src_text):
        # summarize and decode
        batch = self.sum_tokenizer(src_text, truncation=True, padding='longest', return_tensors="pt").to(self.device)
        translated = self.sum_model.generate(**batch)
        tgt_text = self.sum_tokenizer.batch_decode(translated, skip_special_tokens=True)
        return tgt_text[0]

    def process(self, df, content_header = 'generated_abstract'):
        """
        TODO: possible parallelize
        """
        summaries = {}
        for cluster_id in tqdm(df.cluster_idx.unique(), desc='Summarizing'):
            if cluster_id==-1: continue
            # list_of_titles = df.loc[df.cluster_idx==cluster_id, 'title'].tolist() 
            list_of_titles = df.loc[df.cluster_idx==cluster_id, content_header].tolist() 
            
            if len(list_of_titles)>1:
                titles_concat = ' '.join( list_of_titles )
                summary = self.summarize([titles_concat])
                summaries[cluster_id] = summary
            else:
                summaries[cluster_id] = list_of_titles[0]
        summary_df = self.add_cluster_summaries(df, summaries)
        return summary_df

    def add_cluster_summaries(self, df, summaries):
        """
        :param clusters: list of tuples signifying the rows to their clusters
        """
        df_ = df.copy().assign(cluster_summary = np.NaN)
        # DBSCAN gives -1 clusters for unassigned,
        df_.loc[df.cluster_idx==-1,'cluster_summary'] = df.loc[df.cluster_idx==-1,'title']
        for cluster_idx, summary in summaries.items():
            df_.loc[df_.cluster_idx==cluster_idx, 'cluster_summary'] = summary
            
        assert df_.cluster_summary.isnull().sum()==0, "not all articles are assigned a cluster"
        return df_

