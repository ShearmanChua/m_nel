from .parse_date import parse_date_from_filename
from .parse_article_category import parse_tagging
from .parse_article_name import filepath_to_article_name

class RawArticle:
    """
    TODO:
    get filepath of images & their captions, typically <img> and <p> <em> tags in consecutive lines
    """
    def __init__(self, filename, article_soup):
        self.filename = filename
        self.date = parse_date_from_filename(filename)
        self.tagging = parse_tagging(filename)
        self.parse_soup(article_soup)
        
    def parse_soup(self, article_soup):
        headers = article_soup.findAll('h1')
        self.title = headers[0].text if headers else filepath_to_article_name(self.filename)
        self.content = [para.text for para in article_soup.findAll('p') if para.text.strip()]
        
    def to_dict(self):
        # return {'filename':self.filename, 'title':self.title, 'content':self.content, 'date': str(self.date)}
        return {
            'url':self.filename, 
            'title':self.title, 
            'maintext': " \n".join(self.content), 
            'date_publish': str(self.date), 
            "domain_tag":self.tagging}