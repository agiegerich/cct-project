from pyspark import SparkConf, SparkContext 
import functions as f

with open('part0001', 'r') as myfile:
    data=myfile.read()


conf = SparkConf()
conf.setMaster('yarn-client')

sc = SparkContext(conf = conf)

# open part0001
with open('part0001', 'r') as myfile:
    data=myfile.read()

article_rdd = sc.parallelize(f.get_articles(data))
article_rdd = article_rdd.map( lambda article: f.parse_article(article) )

# print out some data we got
stuff = article_rdd.take(40)
for x in stuff:
    print('Title: '+x.title)
    print('\tMCD: ('+x.most_common_date[0]+', '+str(x.most_common_date[1])+')')
    '''
    for l in x.links:
        print('\tArticle Name: '+l.name)
        print('\tDisplay Name: '+l.display)
        print('')
    ''' 
