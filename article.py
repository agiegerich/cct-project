import re
import functions as f

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
    def __init__(self, title, links, most_common_date):
        self.title = title
        self.links = links
        # In the format (date, appearance_count)
        self.most_common_date = most_common_date

class Parser:

    def __init__(self):
        self.mcd_dict = {}
        self.mcd_max_kv = None


    def build_most_common_date(self, word):
        if not f.is_date(word):
            return
        
        date = f.get_date(word)

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

