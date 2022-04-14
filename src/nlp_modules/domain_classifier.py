import tensorflow as tf
import tensorflow_addons as tfa
from joblib import dump, load

from .encoders.use import USE
from .abstract_classes import Pipe

class Domain_Extractor(Pipe):

    def __init__(self, encoder=None):
        if encoder:
            self.encoder=encoder
        else:
            self.encoder = USE()

        # change labels according to the classes
        self.class_labels = ["HADR", "Territory", "T3", "Piracy", ""]
        self.clf = load("resources/models/clf/v3.svm")

        # self.class_labels= ['business', 'entertainment', 'politics', 'sport', 'tech']
        # self.clf = load("resources/models/clf/v4.bbc.5classes.svm")

    def classify_text(self, text):
        
        # get scores
        encoding = self.encoder.encode([text])
        # scores = clf(encoding).numpy()
        scores = self.clf.predict(encoding)
        
        # get label

        label = self.class_labels[ int(scores[0]) ]
            
        return label

    def process(self, df, content_header = 'generated_abstract'):
        domain_df = df.assign( domain_tag = getattr(df, content_header).apply(lambda x: self.classify_text(x)) )
        return domain_df