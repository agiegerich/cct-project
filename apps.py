from pyspark import SparkConf, SparkContext 
import ConfigParser
import re
import bisect
import functions as f
import ast

def get_date_lines_rdd(sc):
    # format is (title, [dates])
    return sc.pickleFile('s3n://agiegerich-wiki-text/date_lines_attempt2/*')

def save_article_to_periods(sc):
    date_lines = get_date_lines_rdd(sc)
    article_periods = date_lines.map(lambda (title, dates): (title, f.get_period(dates))).filter(lambda (title, period): period is not None);
    article_periods.saveAsPickleFile('s3n://agiegerich-wiki-text/article_periods_full_1/')

def get_date_periods(sc):
    return sc.pickleFile('s3n://agiegerich-wiki-text/article_periods_full_1/part*')

def get_article_lines_rdd(sc):
    text_rdd=sc.textFile('s3n://cs5630s17-instructor/wiki-text/part*')
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

    # assign a title 
    articles_rdd = articles_rdd.map( lambda (title_index, line_list): (f.parse_title(line_list[0]), line_list))

    return articles_rdd

def parse_links(sc):
    articles_rdd = get_article_lines_rdd(sc)
    # join all the lines in the same article into one block of text and then parse it into an article
    articles_rdd = articles_rdd.map( lambda (title, line_list): (title, f.parse_links('\n'.join(line_list))))
    articles_rdd.saveAsPickleFile('s3n://agiegerich-wiki-text/title-to-links-map')

def isolate_date_lines_with_context(sc):
    articles_rdd = get_article_lines_rdd(sc)

    date_line_rdd = articles_rdd.map( lambda (title, line_list): (title, f.extract_dates(line_list)))
    #date_line_rdd = articles_rdd.map( lambda (title, line_list): (title, f.isolate_date_lines(line_list)))

    # get rid of empty information
    dates_rdd = date_line_rdd.filter(lambda(title, date_list): len(date_list) > 0)

    '''
    stuff = dates_rdd.takeSample(False, 10)
    for (title, dates) in stuff:
        print(title)
        for d in dates:
            print('\t'+d)
    '''

    dates_rdd.saveAsPickleFile('s3n://agiegerich-wiki-text/date_lines_attempt2')

ids_to_title_map_loc = 's3n://agiegerich-wiki-text/id-title-assignment-full-1/'
def get_ids_to_title_map(sc):
    return sc.pickleFile(ids_to_title_map_loc)

def map_ids_to_title_and_save(sc): 
    articles_rdd = get_article_lines_rdd(sc)
    # join all the lines in the same article into one block of text and then parse it into an article
    articles_rdd = articles_rdd.map( lambda (title_index, line_list): f.parse_title(line_list[0]) ).distinct()

    articles_rdd = articles_rdd.zipWithIndex().map( lambda (title, index): (index, title)) 
    stuff = articles_rdd.take(10)

    articles_rdd.saveAsPickleFile(ids_to_title_map_loc)
