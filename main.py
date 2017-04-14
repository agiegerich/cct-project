from pyspark import SparkConf, SparkContext 
import functions as f
import ConfigParser

config = ConfigParser.ConfigParser()
config.read('config.txt')

#aws_id = SECRET
#aws_key = SECRET
aws_id = config['aws_id']
aws_key = config['aws_key']

spark_conf = SparkConf()
spark_conf.setMaster('yarn-client')
sc = SparkContext(conf = spark_conf)

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", aws_id)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", aws_key)

# open part0001
#with open('part0001', 'r') as myfile:
#    data=myfile.read()

text_rdd=sc.textFile('s3n://cs5630s17-instructor/wiki-text/part0001.gz')

#article_rdd = sc.parallelize(f.get_articles(data))
article_rdd = text_rdd.flatMap( lambda text: f.get_articles(text) )
article_rdd = article_rdd.map( lambda article: f.parse_article(article) )

# print out some data we got
stuff = article_rdd.take(10)
for x in stuff:
    print('Title: '+x.title)
    print('\tMCD: ('+x.most_common_date[0]+', '+str(x.most_common_date[1])+')')
    '''
    for l in x.links:
        print('\tArticle Name: '+l.name)
        print('\tDisplay Name: '+l.display)
        print('')
    ''' 
