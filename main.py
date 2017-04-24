from pyspark import SparkConf, SparkContext 
import ConfigParser
import re
import bisect
import functions as f
import apps
import pr2

config = ConfigParser.RawConfigParser()
config.read('config.cfg')

aws_id = config.get('DEFAULT','aws_id')
aws_key = config.get('DEFAULT', 'aws_key')

spark_conf = SparkConf()
spark_conf.setMaster('yarn-client')
sc = SparkContext(conf = spark_conf)

sc.addPyFile('dependencies.zip')

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", aws_id)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", aws_key)

#apps.test(sc)
#apps.map_articles_to_dates(sc)
#apps.test(sc)
x = apps.get_date_lines_rdd(sc).take(15)
for title, dates in x:
    print(title)
    for d in dates:
        print('\t' + str(d))

#apps.save_article_to_periods(sc)

#x = apps.get_date_periods(sc)
#stuff = x.take(10)
#for (title, period) in stuff:
#    print(title)
#    print(period)
