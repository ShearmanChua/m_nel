import requests
import json 
from datetime import datetime
from time import sleep

from tqdm import tqdm
import pandas as pd
from gdeltdoc import GdeltDoc, Filters
from newsplease import NewsPlease
import newspaper
from newspaper.article import ArticleException, ArticleDownloadState

ARTICLE_INFO_KEYS = ['authors',
                    'date_download',
                    'date_modify',
                    'date_publish',
                    'description',
                    'filename',
                    'image_url',
                    'language',
                    'localpath',
                    'source_domain',
                    'maintext',
                    'title',
                    'title_page',
                    'title_rss',
                    'url']

SELECTED_ARTICLES_DOMAINS = [
    'straitstimes.com',
    'asia.nikkei', 
    'bbc.com', 
    'nst.com', 
    'reuters.com', 
    'abc.net', 
    'channelnewsasia.com', 
    'scmp.com'
    ]

def query_gdelt(keyword=None, timespan=15, domain=SELECTED_ARTICLES_DOMAINS):
    """
    See filters settings here:
    https://github.com/alex9smith/gdelt-doc-api/blob/master/gdeltdoc/filters.py#L54
    
    Possibly useful fields:
    1. start_date, end_date
    2. country
    3. theme
    4. near
    5. repeat
    """
    if type(domain)==str: domain=list(domain)
    f = Filters(
        timespan=timespan,
        num_records = 250,
        keyword = keyword,
        domain = domain,
    )
    gd = GdeltDoc()

    articles = gd._query('artlist', f.query_string)
    if type(articles)==int:
        return pd.DataFrame()
    elif "articles" in articles:
        return pd.DataFrame(articles["articles"])
    else:
        return pd.DataFrame()

def search_gdelt(query):
    """
    Sample usage:
    -----------
    content = search_gdelt("middle rocks")

    Sample output:
    -------------
    {'articles': [{'url': 'https://www.torontotelegraph.com/news/269806474/exploration-crews-mobilize-to-the-akie-property',
    'title': 'Exploration Crews Mobilize to the Akie Property',
    'seendate': '20210608T081821Z',
    'socialimage': '',
    'domain': 'torontotelegraph.com',
    'language': 'ENGLISH',
    'isquote': 0,
    'sentence': 'The deposit is hosted by siliceous, carbonaceous, fine-grained clastic rocks of the Middle to Late Devonian Gunsteel Formation.',
    'context': 'Drilling on the Akie prop .... '},
    """
    query = query.replace(' ', '%20')
    res = requests.get(f"https://api.gdeltproject.org/api/v2/context/context?format=html&query={query}&mode=artlist&maxrecords=75&format=json")
    content = json.loads(res.content)
    return content

def newsplease(url):
    article = NewsPlease.from_url(url)
    return article

def newsplease_article_urls(article_urls, jsonify=False):
    """
    :param article_urls: iterable of url strings
    """
    articles = []
    for link in tqdm(article_urls, desc="Scraping articles"):
        try:
            article = newsplease(link)
            if jsonify: article = jsonify_article(article)
            articles.append(article)
        except Exception as e:
            """
            Expected errors include download error or expired article
            TODO: handle specific errors
            """
            print(e)
            pass
    return articles

def jsonify_article(article, misc=None):
    """
    Enforce fields can be encoded into JSON format.
    """
    json_article = {}
    for key in ARTICLE_INFO_KEYS:
        attribute = getattr(article, key)
        
        # assert attribute type
        if type(attribute)==datetime:
            attribute = str(attribute)
        elif type(attribute) not in [type(None), str, list]:
            print(f"Type not recognized{type(attribute)}")
            
        json_article[key] = getattr(article, key)
    json_article['misc'] = misc
    return json_article

def download_article(link):
    """
    https://github.com/codelucas/newspaper/issues/539
    """
    article = newspaper.Article(url=link)
    slept=0
    article.download()
    while article.download_state == ArticleDownloadState.NOT_STARTED:
        # Raise exception if article download state does not change after 10 seconds
        if slept > 9:
            raise ArticleException('Download never started')
        sleep(1)
        slept += 1

    # Parse article
    parsed = article.parse()
    return parsed