import pandas as pd

def filter_k_sents(title, content, nlp, sents=6):
    text = title

    try: 
        doc = nlp(content)
        for sent in list(doc.sents)[:sents]:
            text += ' ' + sent.text
    except:
        return title
    return text

def row2str(row, nlp, content_header='maintext', sents=6):
    """
    Handles row of a dataframe as pandas series
    :param row: pandas series containing title and content
    """
    title = row.title
    content = getattr(row, content_header)
    text = filter_k_sents(title, content, nlp, sents=sents)
    # print(row)
    # print(text)
    return text #, pd.to_datetime(row.date)
