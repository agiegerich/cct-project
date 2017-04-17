import re

class Const:
    # 
    max_words_in_date = 3
    month = '(?:January|February|March|May|June|July|August|September|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Nov|Dec)'
    day = '(?:(?:[1-2][0-9])|(?:3[0-1])|[1-9])'
    year = '(?:[1-9][0-9]*)'
    line_contains_date_regex = [
        # this will match BC as well as BCE
        (re.compile('([1-9][0-9]* BC )'), 'BC'),
        # AD/CE must be followed by a space
        (re.compile('([1-9][0-9]* (?:AD|CE) )'), 'AD'),
        # AD must be preceded by a space
        (re.compile('( AD [1-9][0-9]{0,2})'), 'AD'),
        (re.compile('('+day+' '+month+' '+year+')'), 'AD'),
        (re.compile('('+month+' '+day+', '+year+')'), 'AD'),
        (re.compile('('+month+' '+year+')'), 'AD'),
        # ranges give a lot of bogus dates
        #(re.compile('([1-9][0-9]{0,3}-[1-9][0-9]{0,3})'), 'AD'),
        # probably the most common word preceding a date
        (re.compile('(in [1-9][0-9]{0,3})'), 'AD'),
        (re.compile('(by [1-9][0-9]{0,3})'), 'AD'),
        # of gives a lot of bogus dates
        #(re.compile('(of [1-9][0-9]{0,3})'), 'AD'),
        # born
        (re.compile('(b\. [1-9][0-9]{0,3})'), 'AD'),
        # circa
        (re.compile('(c\. [1-9][0-9]{0,3})'), 'AD'),
        # died
        (re.compile('(d\. [1-9][0-9]{0,3})'), 'AD'),
        (re.compile('(circa [1-9][0-9]{0,3})'), 'AD'),
        (re.compile('(from [1-9][0-9]{0,3})'), 'AD'),
        (re.compile('(after [1-9][0-9]{0,3})'), 'AD'),
        (re.compile('(before [1-9][0-9]{0,3})'), 'AD'),
        (re.compile('(since [1-9][0-9]{0,3})'), 'AD'),
        # 1000-2099, after the current year and a little into the future, it's less likely to be a date
        (re.compile('( (?:1[0-9][0-9][0-9])|(?:20[0-9][0-9]) )'), 'AD'),
    ]
    date_regex = re.compile('\A[,.!?\(]*[1-2]?[0-9]?[0-9][0-9][,.!?\)]*\Z')
    date_numbers_regex = re.compile('([1-2]?[0-9]?[0-9][0-9])')
    max_year = 2999
    period = 5
