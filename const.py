import re

class Const:
    # 
    max_words_in_date = 3
    month = '(?:January|February|March|May|June|July|August|September|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Nov|Dec)'
    day = '(?:(?:[1-2][0-9])|(?:3[0-1])|[1-9])'
    year = '(?:[1-9][0-9]*)'
    year_matching = '([1-9][0-9]*)'
    period = 5
    references_sections = re.compile('^ *=+ *References *=+ *$')

    dmy = (re.compile('('+day+' '+month+' '+year+')'), 'AD')
    mdy = (re.compile('('+month+' '+day+', '+year+')'), 'AD')

    dmy_matching = (re.compile('('+day+' '+month+' '+year_matching+')'), 'AD')
    mdy_matching = (re.compile('('+month+' '+day+', '+year_matching+')'), 'AD')
    
    with_day_regex = [
        dmy_matching,
        mdy_matching
    ]
    min_bc_ratio = 0.075
    bc_regex = re.compile('([1-9][0-9]* (?:BC|BCE) )')
    line_contains_date_regex = [
        # this will match BC as well as BCE
        (re.compile('([1-9][0-9]* (?:BC|BCE) )'), 'BC'),
        # AD/CE must be followed by a space
        (re.compile('([1-9][0-9]* (?:AD|CE) )'), 'AD'),
        # AD must be preceded by a space
        (re.compile('( AD [1-9][0-9]{0,2})'), 'AD'),
        dmy,
        mdy,
        (re.compile('('+month+' '+year+')'), 'AD'),
        # ranges give a lot of bogus dates
        #(re.compile('([1-9][0-9]{0,3}-[1-9][0-9]{0,3})'), 'AD'),
        # probably the most common word preceding a date
        (re.compile('([iI]n [1-9][0-9]{0,3})'), 'AD'),
        #(re.compile('([^0-9] [bB]y [1-9][0-9]{0,3})'), 'AD'),
        #(re.compile('( of [1-9][0-9]{0,3})'), 'AD'),
        (re.compile('( [fF]rom [1-9][0-9]{0,3} to )'), 'AD'),
        (re.compile('( throughout [1-9][0-9]{0,3})'), 'AD'),
        # born
        #(re.compile('(b\. [1-9][0-9]{0,3})'), 'AD'),
        # circa
        #(re.compile('(c\. [1-9][0-9]{0,3})'), 'AD'),
        # died
        #(re.compile('(d\. [1-9][0-9]{0,3})'), 'AD'),
        (re.compile('(circa [1-9][0-9]{0,3})'), 'AD'),
        #(re.compile('([aA]fter [1-9][0-9]{0,3})'), 'AD'),
        #(re.compile('([bB]efore [1-9][0-9]{0,3})'), 'AD'),
        (re.compile('([sS]ince [1-9][0-9]{0,3})'), 'AD')
        # 1000-2099, after the current year and a little into the future, it's less likely to be a date
        #(re.compile('( (?:1[0-9][0-9][0-9])|(?:20[0-2][0-9]) )'), 'AD'),
    ]
    date_regex = re.compile('\A[,.!?\(]*[1-2]?[0-9]?[0-9][0-9][,.!?\)]*\Z')
    date_numbers_regex = re.compile('([1-2]?[0-9]?[0-9][0-9])')
    max_year = 2999
