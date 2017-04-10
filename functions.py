import re

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
    def __init__(self, links):
        self.links = links


with open('part0001', 'r') as myfile:
    data=myfile.read()

article_delim = re.compile('\$\$\$===cs5630s17===\$\$\$===Title===\$\$\$')
def get_articles(chunk): 
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
    return Article(links) 

# parses a chunk of wikipedia markup to extract articles, links, etc.
def parse(chunk):
    article_dict = {}
    all_articles = get_articles(chunk)
    for article in all_articles:
        title = parse_title(article)
        article_dict[title] = parse_article(article)
    return article_dict



"""
article_dict = parse(data)

x = True
for key in sorted(article_dict.keys()):
    print("TITLE: "+key)
    if x:
        for l in article_dict[key].links:
            print(l.name + ' ' + l.display)
    x = False
    break
"""
