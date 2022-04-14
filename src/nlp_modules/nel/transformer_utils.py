import torch
from torch.utils.data import DataLoader, TensorDataset

class InputExample(object):
    """A single training/test example for simple sequence classification."""

    def __init__(self, guid, text_a, text_b, text_c=None, label=None):
        """Constructs a InputExample.

        Args:
            guid: Unique id for the example.
            text_a: string. The untokenized text of the first sequence. For single
            sequence tasks, only this sequence must be specified.
            text_b: (Optional) string. The untokenized text of the second sequence.
            Only must be specified for sequence pair tasks.
            label: (Optional) string. The label of the example. This should be
            specified for train and dev examples, but not for test examples.
        """
        self.guid = guid
        self.text_a = text_a
        self.text_b = text_b
        self.text_c = text_c
        self.label = label

    def __str__(self):
        return f"{self.text_a} \n {self.text_b} \n {self.text_c} \n"
    
    def __repr__(self):
        return f"{self.text_a} \n {self.text_b} \n {self.text_c} \n"

class InputFeatures(object):
    """A single set of features of data."""

    def __init__(self, input_ids, input_mask, segment_ids, label_id):
        self.input_ids = input_ids
        self.input_mask = input_mask
        self.segment_ids = segment_ids
        self.label_id = label_id
        
def tensorize_features(features):
    all_input_ids = torch.tensor([f.input_ids for f in features], dtype=torch.long)
    all_input_mask = torch.tensor([f.input_mask for f in features], dtype=torch.long)
    all_segment_ids = torch.tensor([f.segment_ids for f in features], dtype=torch.long)
    all_label_ids = torch.tensor([f.label_id for f in features], dtype=torch.long)
    dataset = TensorDataset(all_input_ids, all_input_mask, all_segment_ids, all_label_ids)
    return dataset

def _truncate_seq_pair(tokens_a, tokens_b, tokens_c, max_length):
    """Truncates a sequence pair in place to the maximum length."""

    # This is a simple heuristic which will always truncate the longer sequence
    # one token at a time. This makes more sense than truncating an equal percent
    # of tokens from each, since if one sequence is very short then each token
    # that's truncated likely contains more information than a longer sequence.
    while True:
        total_length = len(tokens_a) + len(tokens_b) + len(tokens_c)
        if total_length <= max_length:
            break
        else:
            # make sentential context and entity context even
            if len(tokens_b) > len(tokens_c):
                tokens_b.pop()
            else:
                tokens_c.pop()

            # # doesnt make sense to only truncate the entity context
            # if len(tokens_c) > 1:
            #     tokens_c.pop()
            # else:
            #     print("TokenB")
            #     tokens_b.pop()
            #     # # doesnt make sense to truncate the mention
            #     # print("TokenA")
            #     # tokens_a.pop()
                
def convert_example_to_feature_roberta(example, tokenizer, max_seq_length=256):
    
    # roberta params
    pad_token=1
    cls_token = '<s>'
    sep_token = '</s>'
    cls_token_segment_id = 0
    pad_token_segment_id = 0
    sep_token_extra = True
    pad_on_left=False
    sequence_a_segment_id=0
    sequence_b_segment_id=1
    sequence_c_segment_id=2
    output_mode='classification'
    
    # tokenize mention, source_text and entity_descriptions
    tokens_a = tokenizer.tokenize(example.text_a)
    tokens_b = tokenizer.tokenize(example.text_b)
    tokens_c = tokenizer.tokenize(example.text_c)
    
    # Modifies `tokens_a` and `tokens_b` in place so that the total
    # length is less than the specified length.
    special_tokens_count = 4
    _truncate_seq_pair(tokens_a, tokens_b, tokens_c, max_seq_length - special_tokens_count)
    
    # The convention in BERT is:
    # (a) For sequence pairs:
    #  tokens:   [CLS] is this jack ##son ##ville ? [SEP] no it is not . [SEP]
    #  type_ids:   0   0  0    0    0     0       0   0   1  1  1  1   1   1
    # (b) For single sequences:
    #  tokens:   [CLS] the dog is hairy . [SEP]
    #  type_ids:   0   0   0   0  0     0   0
    #
    # Where "type_ids" are used to indicate whether this is the first
    # sequence or the second sequence. The embedding vectors for `type=0` and
    # `type=1` were learned during pre-training and are added to the wordpiece
    # embedding vector (and position vector). This is not *strictly* necessary
    # since the [SEP] token unambiguously separates the sequences, but it makes
    # it easier for the model to learn the concept of sequences.
    #
    # For classification tasks, the first vector (corresponding to [CLS]) is
    # used as as the "sentence vector". Note that this only makes sense because
    # the entire model is fine-tuned.
    tokens = tokens_a + [sep_token]
    segment_ids = [sequence_a_segment_id] * len(tokens)
    
    tokens += tokens_b + [sep_token]
    segment_ids += [sequence_b_segment_id] * (len(tokens_b) + 1)

    if tokens_c:
        tokens += tokens_c + [sep_token]
        segment_ids += [sequence_c_segment_id] * (len(tokens_c) + 1)
        
    tokens = [cls_token] + tokens
    segment_ids = [cls_token_segment_id] + segment_ids
    
    input_ids = tokenizer.convert_tokens_to_ids(tokens)

    # The mask has 1 for real tokens and 0 for padding tokens. Only real
    # tokens are attended to.
    input_mask = [1] * len(input_ids)


    # Zero-pad up to the sequence length.
    padding_length = max_seq_length - len(input_ids)
    input_ids = input_ids + ([pad_token] * padding_length)
    input_mask = input_mask + ([0] * padding_length)
    segment_ids = segment_ids + ([pad_token_segment_id] * padding_length)

    assert len(input_ids) == max_seq_length
    assert len(input_mask) == max_seq_length
    assert len(segment_ids) == max_seq_length

    # if example.label is not None:
    #     if output_mode == "classification":
    #         label_id = label_map[example.label]
    #     elif output_mode == "regression":
    #         label_id = float(example.label)
    #     else:
    #         raise KeyError(output_mode)
    # else:
    #     label_id=0

    return InputFeatures(input_ids=input_ids,
                        input_mask=input_mask,
                        segment_ids=segment_ids,
                        # label_id=label_id,
                        label_id=0
                        )
