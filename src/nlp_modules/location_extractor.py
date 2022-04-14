import spacy
from transformers import AutoModelForQuestionAnswering, AutoTokenizer, pipeline
from .abstract_classes import Pipe


class simpleQnA:
    def __init__(self, model_name = "deepset/roberta-base-squad2"):
        """
        https://huggingface.co/deepset/roberta-base-squad2?context=Kuwait%27s+crown+prince+Sheikh+Meshal+al-Ahmad+will+lead+a+delegation%2C+which+includes+the+oil+and+foreign+ministers%2C+on+an+official+visit+to+neighbouring+Saudi+Arabia+on+Tuesday%2C+state+media+reported.&question=where+is+the+event%3F
        
        Sample usage
        ---
        QnA = simpleQnA()
        QnA.ask("where is the event", 
                "Kuwait's crown prince Sheikh Meshal al-Ahmad will lead a delegation, which includes the oil and foreign ministers, on an official visit to neighbouring Saudi Arabia on Tuesday, state media reported.")
        {'score': 0.5686547756195068,
         'start': 152,
         'end': 164,
         'answer': 'Saudi Arabia'}
        """
        self.pipeline = pipeline('question-answering', model=model_name, tokenizer=model_name)
        
    def ask(self, question, context):
        QA_input = {
            'question': question, 
            'context': context
        }
        res = self.pipeline(QA_input)
        return res

def get_ents(nlp, text):
    return nlp(text).ents

def filter_locations(ents):
    return [ ent for ent in ents if ent.label_ in ['GPE', 'LOC'] ]

def guess_location(nlp, QnA, text):
    ents = get_ents(nlp, text)
    locs = filter_locations(ents)
    res = QnA.ask("where is the event?", text)
    locs = [loc.text for loc in locs if loc.text in res['answer']]
    locs = ', '.join(locs)
    return locs

# def get_location_df(df):
#     QnA = simpleQnA()
#     nlp = spacy.load('en_core_web_sm')
#     location_df = df.assign(location_tag = df.description.apply(lambda x: guess_location(nlp, QnA, x)))
#     return location_df

class Location_Extractor(Pipe):
    
    def __init__(self):
        self.QnA = simpleQnA()
        self.nlp = spacy.load('en_core_web_sm')

    def process(self, df):

        # method 1: go row by row
        df.assign(location_tag = '')
        for idx, row in df.iterrows():
            # sometimes the description is null
            # newsplease is unable to retrieve the description
            if type(row.description) == str:
                location_tag = guess_location(self.nlp, self.QnA, row.description)
            else:
                location_tag = guess_location(self.nlp, self.QnA, row.text2encode)
            df.loc[idx, 'location_tag'] = location_tag

        # method 2: just use assign, but this will incur error due to null descriptions
        # location_df = df.assign(location_tag = df.description.apply(lambda x: guess_location(self.nlp, self.QnA, x)))

        # method 3: use assign but use a different input
        # location_df = df.assign(location_tag = df.text2encode.apply(lambda x: guess_location(self.nlp, self.QnA, x)))
        return df