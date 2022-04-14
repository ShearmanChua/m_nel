from abc import ABC, abstractmethod


class Pipe(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def process(self):
        pass

class Idiosyncrasy(ABC):
    @abstractmethod
    def clean(self, df):
        pass

class Clustering_Logic(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def retrieve_articles(self):
        pass
