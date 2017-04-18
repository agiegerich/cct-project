from pyspark import SparkConf, SparkContext 
import ConfigParser
import re
import bisect
import functions as f
import apps
import pr

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
#apps.isolate_date_lines_with_context(sc)
#apps.test(sc)

#apps.save_article_to_periods(sc)
pr.main(sc)

#x = apps.get_date_periods(sc)
#stuff = x.take(10)
#for (title, period) in stuff:
#    print(title)
#    print(period)
