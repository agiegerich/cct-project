import functions as f
from const import Const


class ArticleParser:
    def __init__(self, period):
        self.mcd_dict = {}
        self.mcd_max_kv = None
        self.period = period
        self.mcp = {}

    # most common period
    def build_mcp(self, date):
        (date_as_int, era) = f.extract_date_as_number(date)
        if date_as_int is None:
            return
        if era == 'BC':
            date_as_int *= -1
        
        # add 1 to every date within the period of the given date
        for i in range(date_as_int - self.period, date_as_int + self.period + 1):
            if i not in self.mcp:
                self.mcp[i] = 0
            self.mcp[i] += 1

    def get_mcp(self):
        max_count = 0
        max_year = 0
        for (year, count) in self.mcp.items():
            if count > max_count:
                max_count = count
                max_year = year
        if max_count == 0:
            return None
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

