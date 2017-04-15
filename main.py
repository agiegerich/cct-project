from pyspark import SparkConf, SparkContext 
import ConfigParser
import re
import bisect
#import functions as f

############# article.py ##################
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

###########################################

############# functions.py ################
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
    title = title.replace('$$$===cs5630s17===$$$===Title===$$$', '')
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

###########################################

################ main.py ##################

config = ConfigParser.RawConfigParser()
config.read('config.cfg')

#aws_id = SECRET
#aws_key = SECRET
aws_id = config.get('DEFAULT','aws_id')
aws_key = config.get('DEFAULT', 'aws_key')

spark_conf = SparkConf()
spark_conf.setMaster('yarn-client')
sc = SparkContext(conf = spark_conf)

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", aws_id)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", aws_key)

# open part0001
#with open('part0001', 'r') as myfile:
#    data=myfile.read()



text_rdd=sc.textFile('s3n://cs5630s17-instructor/wiki-text/part000*')
text_rdd=text_rdd.zipWithIndex()


title_line_regex = re.compile('\$\$\$===cs5630s17===\$\$\$===Title===\$\$\$')
title_index_rdd = text_rdd.filter(lambda (line, index): title_line_regex.match(line) is not None) 
title_index_rdd = title_index_rdd.map(lambda (line, index): index)
# relatively small list of integers
indices_of_titles = title_index_rdd.collect()
indices_of_titles.sort()
'''
x = 0
for i in indices_of_titles:
    x += 1
    if x > 20:
        break
    print(i)
'''

# Assigns each line to it's corresponding article title index
text_rdd = text_rdd.map( lambda (line, index): (indices_of_titles[bisect.bisect(indices_of_titles, index) - 1], (line, index) ))

# the title should always come first so this clears out lines that occurred before the first title
text_rdd = text_rdd.filter( lambda (title_index, (line, index)): title_index <= index)

# need to put (line, index) into a list so we can reduce by key using append
text_rdd = text_rdd.map( lambda (title_index, (line, index)): (title_index, [line]) )

# combine all lines from the same article into one
articles_rdd = text_rdd.reduceByKey(lambda a, b: a + b)

articles_rdd = articles_rdd.map( lambda (title_index, line_list): (title_index, parse_article('\n'.join(line_list))))

output_rdd = articles_rdd.map(lambda (title_index, article) : article.title)

output_rdd.saveAsTextFile('/home/agiegerich/cct-project/output')

'''
stuff = articles_rdd.take(1)
for x in stuff:
    x = x[1]
    print('Title: '+x.title)
    print('\tMCD: ('+x.most_common_date[0]+', '+str(x.most_common_date[1])+')')
   # for l in x.links:
   #     print('\tArticle Name: '+l.name)
   #     print('\tDisplay Name: '+l.display)
   #     print('')
'''

'''
stuff = articles_rdd.take(2)
print(stuff[1][1].encode('ascii', 'ignore'))
'''
