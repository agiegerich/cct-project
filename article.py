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
    def __init__(self, title, links, most_common_date, mcp, era):
        self.current_link = 0
        self.title = title
        self.links = links
        # In the format (date, appearance_count)
        self.most_common_date = most_common_date
        # most common period in (begin, end) format
        self.mcp = mcp
        self.era = era

    def __iter__(self):
        return self

    def __next__(self):
        if self.current_link >= len(self.links):
            raise StopIteration
        else:
            self.current += 1
            return self.links[self.current-1]
        

