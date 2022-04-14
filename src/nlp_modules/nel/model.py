import warnings
from tqdm import tqdm
import spacy
import torch
from torch.utils.data import DataLoader, TensorDataset
from transformers import RobertaForSequenceClassification, RobertaTokenizer
from ..abstract_classes import Pipe
from ..falcon.main import process_text_E_R
from .transformer_utils import convert_example_to_feature_roberta, tensorize_features, InputExample
from .wikidata import ES_WikiData

def get_context(ent, width=2):
    """
    Generate context string around the entity of width
    
    :param ent: entity obj from spacy
    :return surrounding_context_string: string containing surrounding sentences 
    """
    doc_sentences = list(ent.doc.sents)
    sentence_id = doc_sentences.index(ent.sent)
    
    # get sentence idx of widths
    min_sent_id = max(sentence_id-width,0)
    max_sent_id = min(sentence_id+width,len(doc_sentences))
    surrounding_context_string = ' '.join([sent.text for sent in doc_sentences[min_sent_id:max_sent_id]])
    
    return surrounding_context_string

class Falcon_Roberta_Disambig(Pipe):
    def __init__(self, model_path='resources/models/ned-discriminator/roberta-wikidisambig.ckpt-22000', host="127.0.0.1", cuda=False):
        self.spacy_model = spacy.load('resources/spacy/en_core_web_sm')
        self.wikidata = ES_WikiData( host=host )
        self.model = RobertaForSequenceClassification.from_pretrained(model_path)
        self.tokenizer = RobertaTokenizer.from_pretrained('resources/roberta/tokenizer')
        self.cuda = cuda
        if cuda:
            self.model = self.model.cuda()
        
    def infer_examples(self, examples, batch_size=2):
        if len(examples) == 0:
            raise ValueError("must have at least one example")
        # preprocess
        input_features = [convert_example_to_feature_roberta(example, self.tokenizer) for example in examples]
        dataset = tensorize_features(input_features)
        
        # run inference in batches
        all_outputs = []
        for step, batch in enumerate( DataLoader(dataset, batch_size=batch_size) ):
            self.model.eval()
            if self.cuda:
                inputs = {'input_ids':    batch[0].cuda(),
                        'attention_mask': batch[1].cuda(),
                        'token_type_ids': None}
            else:
                inputs = {'input_ids':    batch[0],
                        'attention_mask': batch[1],
                        'token_type_ids': None}
                # print(inputs['input_ids'].shape)
            outputs = self.model(**inputs)
            all_outputs.append(outputs.logits)
        # print(f"\n\n   {len(dataset)} - {len(all_outputs)}")
        all_outputs = torch.cat( all_outputs, axis=0 ).detach().cpu().numpy()
        return all_outputs

    def relevent_ent(self, ent):
        return ent.label_ in ['GPE', 'PER', 'ORG'] 
        
    def relevent_loc(self, ent):
        return ent.label_ in ['GPE', 'LOC', 'FAC']  or (ent.label_ == "ORG") and all([k in ent.text.lower() for k in ['air', 'base']]) 
        
    def link_text(self, text, k=10, type='location'):
        assert type in ['location', 'entity'], "link_text type not recognized"

        # 1. Pass through spacy for entities
        doc = self.spacy_model(text)
        if type=='location':
            mentions = [{'doc':ent.doc.text, 'sent':ent.sent.text, 'surface':ent.text, 'surrounding_context':get_context(ent)} for ent in doc.ents if self.relevent_loc(ent)]
        elif type=='entity':
            mentions = [{'doc':ent.doc.text, 'sent':ent.sent.text, 'surface':ent.text, 'surrounding_context':get_context(ent)} for ent in doc.ents if self.relevent_ent(ent)]

        # 2. generate candidates for each surface mention from elastic search
        all_mentions_candidates = []
        for mention in mentions:
            mention_candidates, _ = self.wikidata.generate_candidates(mention['surface'], size=k)
            candidate_id_label = [(candidate['id'],candidate['label']) for candidate in mention_candidates]
            all_mentions_candidates.append(candidate_id_label)

        # # 2. pass through falcon to get viable wikidata entries. some preprocessing required
        # location_mentions = [(ent.sent, ent.text) for ent in doc.ents if self.relevent_ent(ent)]
        # location_mentions = [ ('In ' + location_mention ) if "-" in location_mention else location_mention for location_mention in location_mentions ] # falcon cant seem to handle "-"
        # location_mentions = [ location_mention.replace('\n','').strip() for location_mention in location_mentions ] # clean out next lines
        # location_mentions = [ location_mention.replace("s'",'').strip() for location_mention in location_mentions ] # remove aprostrophe
        # location_mentions = [ location_mention.replace("'s",'').strip() for location_mention in location_mentions ] # remove aprostrophe
        # # troubleshoot falcon
        # for location_mention in location_mentions:
        #     try:
        #         process_text_E_R(location_mention, k=k)
        #     except Exception as e:
        #         print(f"\n\n  {location_mention}")
        #         raise e
        # candidates = [process_text_E_R(location_mention, k=k)[0] for location_mention in location_mentions]
        # # get wikidata labels
        # candidates = [[[one_mention_candidate[0], self.wikidata.get_entity(one_mention_candidate[0])['label']] for one_mention_candidate in one_mention_candidates] for one_mention_candidates in candidates]
        
        # 3. Generate discriminatory seq
        examples = []
        for mention_idx, mention in enumerate( mentions ):
            example = {}
            example['mention_idx'] = mention_idx
            example['mention'] = mention['surface']
            example['candidates'] = all_mentions_candidates[mention_idx]
            example['input_examples'] = []
            for candidate_id, _ in all_mentions_candidates[mention_idx]:
                # example candidate_id, candidate_label: 'Q31', 'Belgium'
                example['input_examples'].append( InputExample(0, mention['surface'], mention['surrounding_context'], self.wikidata.get_graph_from_wikidata_id(candidate_id)) )
                # print( location_mention['surface'], location_mention['sent'], self.wikidata.get_graph_from_wikidata_id(candidate_id) ) 

            # make inference if falcon manages to find candidates
            if len(example['input_examples'])>0:
                try:
                    pred_logits = self.infer_examples(example['input_examples'], batch_size=2)
                except Exception as e:
                    warnings.warn(f"\n error encountered:  {e} \n mention: {example['mention']} \n text: {example} \n")
                    example['mention-qid-label'] = [example['mention'], '', '']
                    examples.append(example)
                    continue
                example['pred_logits'] = pred_logits
                
                # make best guess
                at_least_one_positive_prediction = int(pred_logits.argmax(axis=1).sum())>0
                if at_least_one_positive_prediction:
                    mention_qid_label = [example['mention']]
                    best_score_idx = int(pred_logits[:,1].argmax(axis=0))
                    mention_qid_label.extend(example['candidates'][best_score_idx])

                    example['mention-qid-label'] = mention_qid_label
                else:
                    example['mention-qid-label'] = [example['mention'], '', '']
            else:
                example['mention-qid-label'] = [example['mention'], '', '']
            examples.append(example)
            
        return examples

    def process(self, df, content_header = 'generated_abstract', type='location'):
        df.loc[:,f'{type}_tag'] = ''
        for idx, row in tqdm(df.iterrows(), total=len(df), desc=f"linking {type}"):

            examples = self.link_text( getattr(row, content_header), type=type )
            mention_qid_label = ' | '.join(['%20'.join(example['mention-qid-label']) for example in examples])
            df.loc[idx, f'{type}_tag'] = mention_qid_label

        return df