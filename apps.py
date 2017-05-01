from pyspark import SparkConf, SparkContext 
import ConfigParser
import re
import bisect
import functions as f
import ast
from operator import add

def get_article_lines_rdd(sc, part):
    text_rdd=sc.textFile('/user/hadoop/wiki-text/'+part)
    text_rdd=text_rdd.zipWithIndex()

    title_line_regex = re.compile('\$\$\$===cs5630s17===\$\$\$===Title===\$\$\$')
    title_index_rdd = text_rdd.filter(lambda (line, index): title_line_regex.match(line) is not None) 
    title_index_rdd = title_index_rdd.map(lambda (line, index): index)
    # relatively small list of integers
    indices_of_titles = title_index_rdd.collect()
    indices_of_titles.sort()

    # Assigns each line to it's corresponding article title index
    text_rdd = text_rdd.map( lambda (line, index): (indices_of_titles[bisect.bisect(indices_of_titles, index) - 1], (line, index) ))

    # the title should always come first so this clears out lines that occurred before the first title
    text_rdd = text_rdd.filter( lambda (title_index, (line, index)): title_index <= index)

    # need to put (line, index) into a list so we can reduce by key using append
    text_rdd = text_rdd.map( lambda (title_index, (line, index)): (title_index, [line]) )

    # combine all lines from the same article into one
    articles_rdd = text_rdd.reduceByKey(lambda a, b: a + b)

    # assign a title  and remove the references
    articles_rdd = articles_rdd.map( lambda (title_index, line_list): (f.parse_title(line_list[0]), f.remove_references_and_external_links(line_list)))
    articles_rdd = articles_rdd.filter( lambda (title, line_list): not f.contains_redirect(line_list) )

    return articles_rdd

infobox_data_loc = 's3n://agiegerich-wiki-text/infobox_data/part*'
training_set_loc = 's3n://agiegerich-wiki-text/ground_truth/'
def map_title_to_training_set_date(sc):
    infobox_date_format = re.compile('(-?[0-9]{4})-[0-9]{2}-[0-9]{2}')
    infobox_title_format = re.compile('<http://dbpedia.org/resource/([^>]+)>')
    infobox_data = sc.textFile(infobox_data_loc)
    infobox_data = infobox_data.filter( lambda line: '<http://dbpedia.org/ontology/date>' in line )
    infobox_data = infobox_data.map( lambda line: (f.get_infobox_data(infobox_title_format, line), f.get_infobox_data(infobox_date_format, line)) )
    infobox_data = infobox_data.filter( lambda (title, date): title is not None and date is not None )
    infobox_data = infobox_data.map( lambda (title, date): (title, [int(date)]) )
    infobox_data = infobox_data.reduceByKey(add)
    infobox_data.saveAsPickleFile(training_set_loc)

# (title, [dates])
def get_training_set(sc):
    return sc.pickleFile(training_set_loc)


training_articles_loc = 's3n://agiegerich-wiki-text/training_articles/'
def save_training_articles(sc, part, training):
    # (title, [lines])
    article_lines = get_article_lines_rdd(sc, part+'*')
    # (title, [dates])
    # (title, ([lines], [dates])
    article_lines.join(training).saveAsPickleFile(training_articles_loc+part)


# (title, ([lines], [dates])
def get_training_articles(sc):
    return sc.pickleFile(training_articles_loc+'*')


articles_to_dates_loc = 's3n://agiegerich-wiki-text/articles_to_dates_no_ref_no_redir/'
def pull_article_to_dates_rdd(sc):
    # format is (title, [dates])
    return sc.pickleFile(articles_to_dates_loc+'*/part*')

def local_articles_to_dates(sc, part):
    articles_rdd = get_article_lines_rdd(sc, part) 
    date_line_rdd = articles_rdd.map( lambda (title, line_list): (title, f.extract_dates(line_list)))

    # get rid of empty information
    dates_rdd = date_line_rdd.filter(lambda(title, date_list): len(date_list) > 0)
    return dates_rdd


def train_on_training_data(sc):
    # (title, ([lines], [dates])
    rdd = get_training_articles(sc).map(lambda (title, (line_list, dates)): (title, (f.extract_dates(line_list), dates)))
    rdd = rdd.filter(lambda(title, (date_list, truth_dates)): len(date_list) > 0)
    rdd = rdd.map(lambda (title, (dates, truth_dates)): (title, (f.get_period(dates), truth_dates))).filter(lambda (title, (period, truth_dates)): period is not None);
    total = rdd.count()
    correct = rdd.filter(lambda (title, (period, truth_dates)): period_contains_date(period, truth_dates))
    incorrect = rdd.filter(lambda (title, (period, truth_dates)): not period_contains_date(period, truth_dates))
    correct_count = correct.count()
    for x in incorrect.take(100):
        print(x)
    print('TOTAL: '+str(total))
    print('CORRECT: '+str(correct_count))
    print('ACCURACY: '+str(float(correct_count)/total))


def save_articles_to_dates(sc, part):
    local_articles_to_dates(sc, part).saveAsPickleFile(articles_to_dates_loc+part)


article_to_periods_loc = 's3n://agiegerich-wiki-text/article_periods_no_ref_full_1/'
def local_article_to_periods(sc, local_atd = False, part='part*'):
    if local_atd:
        date_lines = local_articles_to_dates(sc, part)
    else:
        date_lines = pull_article_to_dates_rdd(sc)
    return date_lines.map(lambda (title, dates): (title, f.get_period(dates))).filter(lambda (title, period): period is not None);

def run_full(sc):
    rdd = local_article_to_periods(sc, True, 'part-0071*').map(lambda (title, period): title.replace(',', ' ')+','+str(period[0])+','+str(period[1]))
    rdd.repartition(1).saveAsTextFile(article_to_periods_loc)
    


def save_article_to_periods(sc, local_atd = False):
    local_article_to_periods(sc, local_atd).saveAsPickleFile(article_to_periods_loc)

article_to_periods_text_loc = 's3n://agiegerich-wiki-text/article_periods_no_ref_no_redir_text/'
agiegerich_sep = "$$CCT_AGIEGERICH$$"
def save_article_to_periods_sv(sc, local_atd = False):
    local_article_to_periods(sc, local_atd).map(lambda (title, period): title + agiegerich_sep + str(period[0]) + agiegerich_sep + str(period[1])).saveAsTextFile(article_to_periods_text_loc)


# (title, (start, end))
def pull_date_periods(sc):
    return sc.pickleFile(article_to_periods_loc+'*')

computed_period_vs_truth_loc = 's3n://agiegerich-wiki-text/computed_period_vs_truth1/'
def save_computed_vs_truth(sc, local_atp = False, local_atd = False):
    if local_atp:
        article_to_period = local_article_to_periods(sc, local_atd)
    else:
        article_to_period = pull_date_periods(sc)
    training_set = get_training_set(sc)
    # (title, (start, end)) JOIN (title, [dates]) = (title, ((start, end), dates))
    article_to_period.join(training_set).saveAsPickleFile(computed_period_vs_truth_loc)

def get_computed_vs_truth(sc):
    return sc.pickleFile(computed_period_vs_truth_loc)

def period_contains_date(period, dates):
    for x in range(period[0], period[1]+1):
        if x in dates:
            return True
    return False

def compute_accuracy(sc):
    cvt = get_computed_vs_truth(sc)
    total = cvt.count()
    correctArticles = cvt.filter(lambda (title, (period, dates)): period_contains_date(period, dates))
    incorrectArticles = cvt.filter(lambda (title, (period, dates)): not period_contains_date(period, dates))
    correct = correctArticles.count()
    for x in incorrectArticles.take(300):
        print(x)
    print("total: "+str(total))
    print("correct: "+str(correct))
    print("Accuracy is: "+str(float(correct)/total))

    


def parse_links(sc):
    articles_rdd = get_article_lines_rdd(sc, "*")
    # join all the lines in the same article into one block of text and then parse it into an article
    articles_rdd = articles_rdd.map( lambda (title, line_list): (title, f.parse_links('\n'.join(line_list))))
    articles_rdd.saveAsPickleFile('s3n://agiegerich-wiki-text/title-to-links-map')


ids_to_title_map_loc = 's3n://agiegerich-wiki-text/id-title-assignment-full-1/'
def get_ids_to_title_map(sc):
    return sc.pickleFile(ids_to_title_map_loc)

def map_ids_to_title_and_save(sc): 
    articles_rdd = get_article_lines_rdd(sc, "*")
    # join all the lines in the same article into one block of text and then parse it into an article
    articles_rdd = articles_rdd.map( lambda (title_index, line_list): f.parse_title(line_list[0]) ).distinct()

    articles_rdd = articles_rdd.zipWithIndex().map( lambda (title, index): (index, title)) 
    stuff = articles_rdd.take(10)

    articles_rdd.saveAsPickleFile(ids_to_title_map_loc)
