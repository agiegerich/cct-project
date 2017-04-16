import re

class Const:
    date_regex = re.compile('\A[\(]?[1-2][0-9][0-9][0-9][,.!?\)]?\Z')
    date_numbers_regex = re.compile('([1-2][0-9][0-9][0-9])')
    max_year = 2999
    period = 5
