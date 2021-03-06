import re
from article import Article
from article import Link
from article_parser import ArticleParser
from const import Const

def get_greatest_element_less_than_value(lst, val):
    return indices_of_titles[bisect.bisect(indices_of_titles, index) - 1]

def get_period(dates, era):
    ap = ArticleParser(Const.period, era)
    for d in dates:
        ap.build_mcp(d)
    return ap.get_mcp()

# Gets the likely era for a list of dates.
def get_likely_era(dates, ratio):
    bc_count = 0
    ad_count = 0
    for date in dates:
        regex = Const.bc_regex
        match = re.search(regex, date)
        if match:
            bc_count += 1
        else:
            ad_count += 1
    if bc_count == 0:
        return 'AD'
    elif ad_count == 0:
        return 'BC'
    elif float(bc_count)/ad_count >= ratio:
        return 'BC'
    else:
        return 'AD'

def extract_date_as_number(date):
    # handle all the dates with the month and day in them
    for (regex, era) in Const.with_day_regex:
        match = re.search(regex, date)
        if match:
            return (int(match.group(2)), era)
    # all other dates should only have one number, the year
    for (regex, era) in Const.line_contains_date_regex:
        if re.search(regex, date):
            match = re.search(Const.year_matching, date) 
            if match:
                return (int(match.group(1)), era)
    return None

def is_date(word):
    if Const.date_regex.match(word):
        return True
    else:
        return False

def get_infobox_data(regex, line):
    matches = re.findall(regex, line)
    for m in matches:
        return m
    return None

def contains_redirect(line_list):
    for l in line_list:
        if '#REDIRECT' in l:
            return True
    return False

def get_articles(chunk): 
    article_delim = re.compile('\$\$\$===cs5630s17===\$\$\$===Title===\$\$\$')
    articles = re.split(article_delim, chunk)
    articles = filter(lambda a : (a is not None) and (len(a.strip())) > 0, articles)
    articles = map(lambda a : a.replace('$$$===cs5630s17===$$$===cs5630s17===$$$', ''), articles)
    return articles

def parse_title(article):
    title=''
    for c in article:
        if c == '\n':
            break
        title += c
    title = title.replace('$$$===cs5630s17===$$$===Title===$$$', '')
    return title.strip()

# Removes the references section and everything below it.
def remove_references_and_external_links(article_lines): 
    for i, line in enumerate(article_lines):
        if re.match(Const.references_sections, line):
            return article_lines[:(i-1 if i-1 >= 1 else 1)]
    return article_lines

# Convert text inside a link tag to a Link object.
def convert_text_to_link(link_text):
    link_names = link_text.split('|')
    if len(link_names) == 0:
        return "INVALID-LINK"
    else:
        return link_names[0]
    '''
    elif len(link_names) == 1:
        return Link.create(link_names[0]) 
    elif len(link_names[1]) > 0:
        return Link.create_with_display(link_names[0], link_names[1])
    ''' 

# Gets all the links from an article.
def parse_links(article_text):
    links = re.findall('\[\[[^[\]]+\]\]', article_text)
    links = map(lambda l: l.replace('[[', ''), links)
    links = map(lambda l: l.replace(']]', ''), links)
    links = map(lambda l: convert_text_to_link(l).strip().replace(' ', '_'), links)
    return links


# parses an article to extract links, etc.
def parse_article(article_text):
    links = parse_links(article_text)
    words = article_text.split()
    parser = ArticleParser(Const.period)
    for word in words:
        parser.build_most_common_date(word)
        parser.build_mcp(word)
    return Article(parse_title(article_text), links, parser.get_most_common_date(), parser.get_mcp()) 

# parses a chunk of wikipedia markup to extract articles, links, etc.
def parse(chunk):
    article_dict = {}
    all_articles = get_articles(chunk)
    for article in all_articles:
        title = parse_title(article)
        article_dict[title] = parse_article(article)
    return article_dict

def extract_dates(line_list):
    dates = []
    for line in line_list:
        for regex, era in Const.line_contains_date_regex:
            matches = re.findall(regex, line)
            for m in matches:
                dates.append(m)
                # separator is just something that won't be parsed as a date. We don't want dates "combining" into new dates
                line = line.replace(m, 'SEPARATOR') 
    return dates
