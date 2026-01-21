import sqlparse
from abc import ABC, abstractmethod


class BaseSQLChecker(ABC):

    @abstractmethod
    def __init__(self, phrases):
        self.phrases = phrases

    @abstractmethod
    def check_file(self, sql_file):
        pass

    @abstractmethod
    def sql_phrase_checker(self, phrase, clean_statement):
        pass

    @staticmethod
    def parse_sql_file(sql_file):
        with open(sql_file, 'r') as file_read:
            sql_str = file_read.read().lower()
            return sqlparse.split(sqlparse.format(sql_str, strip_comments=True, strip_whitespace=True))
