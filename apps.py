from pyspark import SparkConf, SparkContext 
import ConfigParser
import re
import bisect
import functions as f
import ast
from operator import add
import math
from const import Const

# Gets and RDD of each article mapped  to the list of lines in that article.
def get_article_lines_rdd(sc, part):
    #text_rdd=sc.textFile('/user/hadoop/wiki-text/'+part)
    text_rdd=sc.textFile('s3n://cs5630s17-instructor/wiki-text/'+part)
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
# builds an rdd containing a title and the "true" dates from that article and saves it as a pickle file.
def map_title_to_training_set_date(sc):
    infobox_date_format = re.compile('(-?[0-9]{4})-0-9]{2}-[0-9]{2}')
    infobox_title_format = re.compile('<http://dbpedia.org/resource/([^>]+)>')
    infobox_data = sc.textFile(infobox_data_loc)
    infobox_data = infobox_data.filter( lambda line: '<http://dbpedia.org/ontology/date>' in line )
    infobox_data = infobox_data.map( lambda line: (f.get_infobox_data(infobox_title_format, line), f.get_infobox_data(infobox_date_format, line)) )
    infobox_data = infobox_data.filter( lambda (title, date): title is not None and date is not None )
    infobox_data = infobox_data.map( lambda (title, date): (title, [int(date)]) )
    infobox_data = infobox_data.reduceByKey(add)
    infobox_data.saveAsPickleFile(training_set_loc)

# gets the an RDD of titles and the corresponding "true" dates for that title.
# (title, [dates])
def get_training_set(sc):
    return sc.pickleFile(training_set_loc)


training_articles_loc = 's3n://agiegerich-wiki-text/training_articles/'
# Builds an RDD of titles of training articles, the "true" dates for that articles, and the lines for that
# articles and saves it to S3 as a pickle file.
def save_training_articles(sc, part, training):
    # (title, [lines])
    article_lines = get_article_lines_rdd(sc, part+'*')
    # (title, [dates])
    # (title, ([lines], [dates])
    article_lines.join(training).saveAsPickleFile(training_articles_loc+part)


# Gets the RDD of titles, true dates, and lines for each training/validation article
# (title, ([lines], [dates])
def get_training_articles(sc):
    return sc.pickleFile(training_articles_loc+'*')


articles_to_dates_loc = 's3n://agiegerich-wiki-text/articles_to_dates_no_ref_no_redir/'
# Gets an RDD of articles and the dates contained in that article from S3.
def pull_article_to_dates_rdd(sc):
    # format is (title, [dates])
    return sc.pickleFile(articles_to_dates_loc+'*/part*')

# Builds an RDD of articles and the dates contained in that article from scratch.
def local_articles_to_dates(sc, part):
    articles_rdd = get_article_lines_rdd(sc, part) 
    date_line_rdd = articles_rdd.map( lambda (title, line_list): (title, f.extract_dates(line_list)))

    # get rid of empty information
    dates_rdd = date_line_rdd.filter(lambda(title, date_list): len(date_list) > 0)
    return dates_rdd

# Returns true if a list of dates contains a BC date.
def contains_bc(dates):
    for d in dates:
        if d < 0:
            return True
    return False

# Main entry point for running the date extraction and counting algorithms on the validation data and reporting the accuracy.
def train_on_training_data(sc, ratio):
    with open('output/'+str(ratio)+'.txt', 'a') as output_file:
        ratio=float(ratio)/100
        output_file.write(str(ratio)+'\n')
        # (title, ([lines], [dates])
        rdd = get_training_articles(sc).map(lambda (title, (line_list, dates)): (title, (f.extract_dates(line_list), dates)))
        rdd = rdd.filter(lambda(title, (date_list, truth_dates)): len(date_list) > 0)
        rdd = rdd.map(lambda(title, (date_list, truth_dates)): (title, (date_list, truth_dates, f.get_likely_era(date_list, ratio))))

        rdd = rdd.map(lambda (title, (dates, truth_dates, era)): (title, (f.get_period(dates, era), truth_dates))).filter(lambda (title, (period, truth_dates)): period is not None);
        total = rdd.count()
        correct = rdd.filter(lambda (title, (period, truth_dates)): period_contains_date(period, truth_dates))
        incorrect = rdd.filter(lambda (title, (period, truth_dates)): not period_contains_date(period, truth_dates))
        correct_count = correct.count()
        bc_incorrect = incorrect.filter(lambda (title, (period, truth_dates)): contains_bc(truth_dates))
        bci_count = bc_incorrect.count()
        bc_correct = correct.filter(lambda (title, (period, truth_dates)): contains_bc(truth_dates))
        bcc_count = bc_correct.count()
        for x in bc_incorrect.take(100):
            print(x)
        
        output_file.write('\tBC CORRECT: '+str(bcc_count) + '\n')
        output_file.write('\tBC TOTAL: '+str(bcc_count+bci_count) + '\n')
        output_file.write('\tBC ACCURACY: '+str(float(bcc_count)/(bcc_count+bci_count)) + '\n')
        output_file.write('\tTOTAL: '+str(total) + '\n')
        output_file.write('\tCORRECT: '+str(correct_count) + '\n')
        output_file.write('\tACCURACY: '+str(float(correct_count)/total) + '\n')

# Saves an RDD of each article and the dates in that article to S3.
def save_articles_to_dates(sc, part):
    local_articles_to_dates(sc, part).saveAsPickleFile(articles_to_dates_loc+part)

# Gets the decade (1940s, 1950s etc.) given a period of time (year, year)
def get_decade(period):
    average = (period[0]+period[1])/2.0
    decade = math.floor(average/10.0)*10
    return int(decade)

article_to_periods_loc = 's3n://agiegerich-wiki-text/article_periods_no_ref_full_final_20/'
# Builds an RDD of each article and the period computed for that article.
def local_article_to_periods(sc, ratio = Const.min_bc_ratio, local_atd = False, part='part*'):
    if local_atd:
        date_lines = local_articles_to_dates(sc, part)
    else:
        date_lines = pull_article_to_dates_rdd(sc)
    rdd = date_lines.map(lambda (title, dates): (title, (dates, f.get_likely_era(dates, ratio))))
    rdd = rdd.map(lambda (title, (dates, era)): (title, f.get_period(dates, era)))
    rdd = rdd.filter(lambda (title, period): period is not None);
    return rdd

# Runs the data extraction and counting on the full dataset and saves the results to S3 as a csv. (title, decade)
def run_full(sc, part):
    rdd = local_article_to_periods(sc, Const.min_bc_ratio, True, part+'*')
    rdd = rdd.map(lambda (title, period): title.replace(',', ' ')+','+str(get_decade(period)))
    rdd.saveAsTextFile(article_to_periods_loc+part)

# Creates and saves an RDD of articles and their computed periods.
def save_article_to_periods(sc, local_atd = False):
    local_article_to_periods(sc, Const.min_bc_ratio, local_atd).saveAsPickleFile(article_to_periods_loc)

# Returns true if a period contains a date.
def period_contains_date(period, dates):
    for x in range(period[0], period[1]+1):
        if x in dates:
            return True
    return False
