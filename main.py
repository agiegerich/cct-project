from pyspark import SparkConf, SparkContext 
import ConfigParser
import re
import bisect
import functions as f
import apps

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
#apps.map_articles_to_dates(sc, 'part-000')
#apps.map_articles_to_dates(sc, 'part-001')
#apps.map_articles_to_dates(sc, 'part-002')
#apps.map_articles_to_dates(sc, 'part-003')
#apps.map_articles_to_dates(sc, 'part-004')
#apps.map_articles_to_dates(sc, 'part-005')
#apps.map_articles_to_dates(sc, 'part-006')
#apps.map_articles_to_dates(sc, 'part-007')
#apps.test(sc)
#apps.map_title_to_training_set_date(sc)
#training_set= apps.get_training_set(sc)
#for x in training_set.take(10):
#    print(x)
#print('COUNT IS: ' + str(training_set.count()))
    
#apps.save_article_to_periods(sc)
#apps.save_computed_vs_truth(sc)
#apps.compute_accuracy(sc)

#training = apps.get_training_set(sc)
#for part in range(0, 75):
#    apps.save_training_articles(sc, 'part-'+str(part).zfill(4), training)

for part in range(0, 8):
    apps.run_full(sc, 'part'+str(part).zfill(2))


#apps.train_on_training_data(sc, 20)

#for (title, period) in stuff:
#    print(title)
#    print(period)
