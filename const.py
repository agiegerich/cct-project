import re

class Const:
    # 
    max_words_in_date = 3
    month = '(?:January|February|March|May|June|July|August|September|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Nov|Dec)'
    day = '(?:(?:[1-2][0-9])|(?:3[0-1])|[1-9])'
    year = '(?:[1-9][0-9]*)'
    year_matching = '([1-9][0-9]*)'
    period = 5

    ground_truth = [
        ('Battle_of_the_Granicus', (-334, -334)),
        ('Battle_of_Ipsus', (-301, -301)),
        ('First_Punic_War', (-264, -241)),
        ('First_Mithridatic_War', (-89, -85)),
        ('Siege_of_Jerusalem_(63_BC)', (-63,-63)),
        ('Battle_of_the_Teutoburg_Forest', (9, 9)),
        ('Great_Fire_of_Rome', (64, 64)),
        ('Eruption_of_Mount_Vesuvius_in_79', (79, 79)),
        ('Bar_Kokhba_revolt', (132,136)),
        ('Yellow_Turban_Rebellion', (184, 205)),
        ('Constitutio_Antoniniana', (212, 212)),
        ('Battle_of_Strasbourg', (357, 357)),
        ('Red_Turban_Rebellion', (1351, 1368))
    ]

    dmy = (re.compile('('+day+' '+month+' '+year+')'), 'AD')
    mdy = (re.compile('('+month+' '+day+', '+year+')'), 'AD')

    dmy_matching = (re.compile('('+day+' '+month+' '+year_matching+')'), 'AD')
    mdy_matching = (re.compile('('+month+' '+day+', '+year_matching+')'), 'AD')
    
    with_day_regex = [
        dmy_matching,
        mdy_matching
    ]
    line_contains_date_regex = [
        # this will match BC as well as BCE
        (re.compile('([1-9][0-9]* BC )'), 'BC'),
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
