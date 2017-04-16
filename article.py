import re
import functions as f
from const import Const

class Link:
    def __init__(self, name, display):
        self.name = name
        self.display = display

    @classmethod
    def create(cls, name):
        return cls(name, name)

    @classmethod
    def create_with_display(cls, name, display):
        return cls(name, display)

class Article:
    def __init__(self, title, links, most_common_date, mcp):
        self.current_link = 0
        self.title = title
        self.links = links
        # In the format (date, appearance_count)
        self.most_common_date = most_common_date
        # most common period in (begin, end) format
        self.mcp = mcp

    def __iter__(self):
        return self

    def __next__(self):
        if self.current_link >= len(self.links):
            raise StopIteration
        else:
            self.current += 1
            return self.links[self.current-1]
        

class Parser:
    def __init__(self, period):
        self.mcd_dict = {}
        self.mcd_max_kv = None
        self.period = period
        self.mcp = [0] * (Const.max_year + 1 + self.period)

    # most common period
    def build_mcp(self, word):
        if not f.is_date(word):
            return

        date_as_int = f.extract_date_as_number(word)
        if date_as_int is None:
            return
        
        # add 1 to every date within the period of the given date
        for i in range(date_as_int - self.period, date_as_int + self.period + 1):
            self.mcp[i] += 1

    def get_mcp(self):
        max_count = 0
        max_year = 0
        for year, count in enumerate(self.mcp):
            if count > max_count:
                max_count = count
                max_year = year
        if max_count == 0:
            return (-1, -1)
        else:
            return (max_year-self.period, max_year+self.period)
            
            

    def build_most_common_date(self, word):
        if not f.is_date(word):
            return
        
        date = f.extract_date_as_number(word)

        if date is None:
            return

        if date in self.mcd_dict:
            self.mcd_dict[date] += 1
            if self.mcd_dict[date] > self.mcd_max_kv[1]:
                self.mcd_max_kv = (date, self.mcd_dict[date])
        else:
            self.mcd_dict[date] = 1
            if self.mcd_max_kv is None:
                self.mcd_max_kv = (date, 1)

    def get_most_common_date(self):
        if self.mcd_max_kv is None:
            return ('N/A', 0)
        else:
            return self.mcd_max_kv

