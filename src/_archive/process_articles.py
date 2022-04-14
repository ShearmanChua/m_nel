import os
import json
import pandas as pd
import argparse
from datetime import datetime
import spacy

from src.pipeline.clustering import MKV, DB
from src.pipeline.summarizer import Pegasus
from src.pipeline.location_extractor import Location_Extractor
from src.pipeline.domain_classifier import Domain_Extractor
from src.pipeline.article_idiosyncrasies import Idiosyncrasy_Cleaner
from src.utils import row2str

if __name__=="__main__":
    # https://docs.python.org/2/library/argparse.html
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", default='input/demo/June1-2-includes-msia-infringement.json', help="input articles in json")
    parser.add_argument("-o", "--output", default='output.xlsx', help="input articles in json")
    args = parser.parse_args()

    # Reading from jsonlines file
    with open(args.input, 'rb') as f:
        lines = f.readlines()
    json_articles = [json.loads(line.decode('utf-8')) for line in lines]
    print( f"{datetime.now()} - File read: {args.input}" )

    # preprocess json articles into df
    df = pd.DataFrame(json_articles)
    df.authors = df.authors.apply(lambda x: ', '.join(x))
    nlp = spacy.load('en_core_web_sm')
    # clear idiosyncrasies
    idio_cleaner = Idiosyncrasy_Cleaner()
    df = idio_cleaner.process(df)
    # generate text2encode column
    # TODO: use df.apply instead -> faster
    df = df.assign(text2encode = [row2str( row, nlp = nlp) for _, row in df.iterrows()])

    # cluster articles
    print(f"{datetime.now()} - Clustering {len(df)} articles")
    # model = MKV()
    model = DB()
    df = model.process(df)
    print(f"{datetime.now()} - Articles clustered")

    # summarize each cluster
    # TODO: to be parallelized
    summarizer_model = Pegasus()
    df = summarizer_model.process(df)
    print(f"{datetime.now()} - All articles summarized")

    # get location
    location_model = Location_Extractor()
    df = location_model.process(df)
    print(f"{datetime.now()} - Location information extracted")

    # classify domain
    domain_model = Domain_Extractor()
    df = domain_model.process(df)
    print(f"{datetime.now()} - Articles classified")

    # save
    df.to_excel(args.output)