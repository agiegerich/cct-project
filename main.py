from pyspark import SparkConf, SparkContext 
import ConfigParser
import re
import bisect
import functions as f

config = ConfigParser.RawConfigParser()
config.read('config.cfg')

#aws_id = SECRET
#aws_key = SECRET
aws_id = config.get('DEFAULT','aws_id')
aws_key = config.get('DEFAULT', 'aws_key')

spark_conf = SparkConf()
spark_conf.setMaster('yarn-client')
sc = SparkContext(conf = spark_conf)

sc.addPyFile('dependencies.zip')

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", aws_id)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", aws_key)

# open part0001
#with open('part0001', 'r') as myfile:
#    data=myfile.read()



text_rdd=sc.textFile('s3n://cs5630s17-instructor/wiki-text/part0001.gz')
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

# join all the lines in the same article into one block of text and then parse it into an article
articles_rdd = articles_rdd.map( lambda (title_index, line_list): (title_index, f.parse_article('\n'.join(line_list))))

articles_rdd = articles_rdd.zipWithIndex().map( lambda ((title_index, article), index): (index, article)) 

stuff = articles_rdd.take(10)

for (index, article) in stuff:
    print('index: '+ str(index))
    print('length: '+str(article.title))
    
#output_rdd = articles_rdd.map(lambda (title_index, article) : article.title)

#output_rdd.saveAsTextFile('/home/agiegerich/cct-project/output')
