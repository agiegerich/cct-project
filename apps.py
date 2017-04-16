from pyspark import SparkConf, SparkContext 
import ConfigParser
import re
import bisect
import functions as f
import ast

def test(sc):
    ids_rdd = sc.pickleFile('s3n://agiegerich-wiki-text/id-title-assignment-test2/part*')

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
    return articles_rdd



def fully_parse(sc):
    articles_rdd = get_article_lines_rdd(sc)
    # join all the lines in the same article into one block of text and then parse it into an article
    articles_rdd = articles_rdd.map( lambda (title_index, line_list): (title_index, f.parse_article('\n'.join(line_list))))

    articles_rdd = articles_rdd.zipWithIndex().map( lambda ((title_index, article), index): (index, article.title)) 

    articles_rdd.saveAsPickleFile('s3n://agiegerich-wiki-text/id-title-assignment')

    '''
    for (index, article) in stuff:
        print('article: '+str(article.title))
        print('mcd: '+str(article.most_common_date))
        print('mcp: '+str(article.mcp))
    '''
        
    #output_rdd = articles_rdd.map(lambda (title_index, article) : article.title)

    #output_rdd.saveAsTextFile('/home/agiegerich/cct-project/output')


def map_ids_to_title_and_save(sc): 
    articles_rdd = get_article_lines_rdd(sc)
    # join all the lines in the same article into one block of text and then parse it into an article
    articles_rdd = articles_rdd.map( lambda (title_index, line_list): (title_index, f.parse_title(line_list[0])))

    articles_rdd = articles_rdd.zipWithIndex().map( lambda ((title_index, title), index): (index, title)) 

    articles_rdd.saveAsPickleFile('s3n://agiegerich-wiki-text/id-title-assignment')
