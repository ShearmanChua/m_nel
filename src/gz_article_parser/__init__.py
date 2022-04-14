import os
import zipfile
from bs4 import BeautifulSoup
from tqdm import tqdm

from .raw_article import RawArticle

def get_html_filenames(archive):
    return [file.filename for file in archive.filelist if file.filename.lower().endswith('.html')]

def read_zipped_articles(zip_folder):
    """
    Read articles from zipped archive
    """
    article_soups = {}

    with zipfile.ZipFile(zip_folder, 'r') as zipped_archive:
        html_filenames = get_html_filenames(zipped_archive)

        for html_filename in html_filenames:
            # read and parse html file
            html_file = zipped_archive.open(html_filename)
            article_soup = BeautifulSoup(html_file.read(), 'lxml')
            article_soups[html_filename] = article_soup
    
    return article_soups

def get_articles_from_trove(article_trove_path = 'data/Scanned'):
    """
    >>> articles = get_articles_from_trove()
    >>> articles_ = [(article.title, article.filename) for article in articles]
    >>> articles_[0]
    ('NYK Tests AI System to Automatically Identify Navigation Hazards',
    'data/Scanned/20210911/Techscan2021091101.zip/AI_IRC/00289981/NYK_Tests_AI_System_to_Automatically_Identify_Navigation_Hazards.HTML')
    """
    zip_folders = []
    for scrape_date in os.listdir(article_trove_path):
        for zip_file_path in os.listdir(article_trove_path + '/' + scrape_date):
            zip_folder = article_trove_path + '/' + scrape_date + '/' + zip_file_path
            zip_folders.append(zip_folder)

    articles = []
    for zip_folder in tqdm(zip_folders, desc = "parsing zip folders into RawArticles, then into .jsonl"):
        article_soups = read_zipped_articles(zip_folder)
        for article_name, article_soup in article_soups.items():
            article_path = zip_folder + '/' + article_name
            articles.append( RawArticle(article_path, article_soup) )
    return articles