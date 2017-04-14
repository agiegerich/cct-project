import re
from article import Article
from article import Link
def is_date(word):
    date_regex = re.compile('\A[1-9][0-9][0-9][0-9][,.!?]\Z')
    if date_regex.match(word):
        return True
    else:
        return False

def get_date(word):
    return word[0:4]

def get_greatest_element_less_than_value(lst, val):
    return indices_of_titles[bisect.bisect(indices_of_titles, index) - 1]


def most_common_date(article):
    max_kv = None
    date_dict = {}
    words = article.split()
    for word in words:
        if not is_date(word):
            continue
        
        date = get_date(word)

        if date in date_dict:
            date_dict[date] += 1
            if date_dict[date] > max_kv[1]:
                max_kv = (date, date_dict[date])
        else:
            date_dict[date] = 1
            if max_kv is None:
                max_kv = (date, 1)
    if max_kv is None:
        return ('N/A', 0)
    else:
        return max_kv


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
    return title.strip()

# Convert text inside a link tag to a Link object.
def convert_text_to_link(link_text):
    link_names = link_text.split('|')
    if len(link_names) == 0:
        return Link.create("INVALID")
    elif len(link_names) == 1:
        return Link.create(link_names[0]) 
    elif len(link_names[1]) > 0:
        return Link.create_with_display(link_names[0], link_names[1])
    else:
        return Link.create(link_names[0])

# Gets all the links from an article.
def parse_links(article_text):
    links = re.findall('\[\[[^[\]]+\]\]', article_text)
    links = map(lambda l: l.replace('[[', ''), links)
    links = map(lambda l: l.replace(']]', ''), links)
    links = map(lambda l: convert_text_to_link(l), links)
    return links

# parses an article to extract links, etc.
def parse_article(article_text):
    links = parse_links(article_text)
    words = article_text.split()
    return Article(parse_title(article_text), links, most_common_date(article_text)) 

# parses a chunk of wikipedia markup to extract articles, links, etc.
def parse(chunk):
    article_dict = {}
    all_articles = get_articles(chunk)
    for article in all_articles:
        title = parse_title(article)
        article_dict[title] = parse_article(article)
    return article_dict
