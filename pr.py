from operator import add
import gc
import sys


def parse_int(id):
    try:
        id = int(id)
    except:
        id = -1
    return id
    
def update_weight_if_has_outgoing(weight, outgoing):
    if outgoing == 0:
        return weight
    else:
        return weight / outgoing
        
def distribute_page_rank(in_list, score_map):
    sum = 0.0
    for src in in_list:
        if src in score_map:
            sum += score_map[src]
    return sum

'''
def distribute_page_rank(in_list, score_rdd):
    return score_rdd.filter(lambda (id, weight_io): id in in_list).reduce(lambda (a, b): a[1][0] + b[1][0])
'''

class PageRankCalculator:
    def __init__(self, page_links_by_source, page_links_by_target, sc):
        # Precondition: Makes sure the two RDDs have unique keys
        
        #self.damp_param = damp_param
        
        # get the articles that have incoming links (and are therefore active)
        active_by_incoming = page_links_by_source.filter(lambda (article_id, links): len(links) > 0).map(lambda (article_id, links): (article_id, (links, [])) )
        # get the articles that have outgoing links (and are therefore active)
        active_by_outgoing = page_links_by_target.filter(lambda (article_id, links): len(links) > 0).map(lambda (article_id, links): (article_id, ([], links)) )
        # reduces to the links that are active with an tuple containing the incoming and outgoing links (in, out)
        self.active_links = sc.union([active_by_incoming, active_by_outgoing]).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
        
       # stuff = self.active_links.take(100)
        #print(stuff)
    
        #instantiate page rank scores, number of active links, and initial weight
    
        self.num_active = self.active_links.count()
        init_weight = 1.0/self.num_active
        
        # store the in and out links in the page rank scores for efficiency
        self.page_rank_scores = self.active_links.map(lambda t: (t[0], (init_weight, t[1])) )
        self.hard_scores = {}
        # We can either do this as a map or as a List. a List might be better...
        #self.hard_scores = self.page_rank_scores.map(lambda (id, (weight, in_out)): (id, weight) ).collectAsMap()
        self.init_hard_scores = False
        

    def run_page_rank(self, damp_param):
        num_active = self.num_active
        
        '''
        if self.init_hard_scores:
            hard_scores_rdd = self.hard_scores_rdd
        self.init_hard_scores=True
        '''
            
        # step 1
        #CHANGED
        hard_scores = self.page_rank_scores.map(lambda (id, (weight, (in_list, out_list))) : (id, update_weight_if_has_outgoing(weight, len(out_list))) ).collectAsMap()
        #hard_scores_rdd = self.page_rank_scores.map(lambda (id, (weight, (in_list, out_list))) : (id, (update_weight_if_has_outgoing(weight, len(out_list)), (in_list, out_list))) )

        # step 3
        # get the combined page ranks for the articles with no outgoing links to compute the bias
        # weight_io_tuple is (weight, (in_list, out_list))

        #CHANGED
        no_out_bias = self.page_rank_scores.filter(lambda (id, weight_io_tuple): len(weight_io_tuple[1][1]) == 0).map(lambda (id, na): hard_scores[id] if id in hard_scores else 0).reduce(add)
        #no_out_bias = hard_scores_rdd.filter(lambda (id, weight_io): len(weight_io[1][1]) == 0).map(lambda (id, weight_io): weight_io[1]).reduce(add)
        
        no_out_bias = no_out_bias/num_active

        # NOTE: I swapped step 2 and 3 as they don't rely on each other and the temp_page_rank_scores aren't needed until step 4
        # step 2, distribute page rank scores
        # weight_io_tuple is (weight, (in_list, out_list))

        # CHANGED
        temp_page_rank_scores = self.page_rank_scores.map(lambda (id, weight_io_tuple): (id, (distribute_page_rank(weight_io_tuple[1][0], hard_scores), (weight_io_tuple[1][0], weight_io_tuple[1][1]))) )
        #temp_page_rank_scores = self.page_rank_scores.map(lambda (id, weight_io): (id, (distribute_page_rank_rdd(weight_io[1][0], hard_scores_rdd), weight_io[1])))
        
        # step 4, 

        # CHANGED
        hard_scores = temp_page_rank_scores.map(lambda (id, weight_io_tuple): (id, weight_io_tuple[0] * damp_param + no_out_bias * damp_param + (1 - damp_param) / num_active ) ).collectAsMap()
        #hard_scores_rdd = temp_page_rank_scores.map(lambda (id, weight_io): (id, ((weight_io[0] * damp_param + no_out_bias * damp_param + (1 - damp_param) / num_active), weight_io[1] ) ) )

        # CHANGED
        self.hard_scores = hard_scores
        gc.collect()
        #self.hard_scores_rdd = hard_scores_rdd


def remove_invalid_links(links):
    new_links = []
    for i in links:
        if i >= 0:
            new_links.append(i)
    return new_links

def main(sc): 
    #data_file_full = '/media/WinShare/provisions\ for\ LDA\ server/pagelinks_sparse.csv'
    #data_file = '/media/WinShare/provisions\ for\ LDA\ server/pagelinks_sparse_sample.csv'
    raw_data = sc.textFile('s3n://agiegerich-lda/pagelinks_sparse.csv')
    # I added the reduceByKey to the end to make sure all article_ids are unique
    pagelinks_by_target = raw_data.map(lambda l: l.split(",")).map(lambda p:(int(p[0]), map(lambda e: parse_int(''.join([c for c in e if c in '1234567890'])), p[2:]))).reduceByKey(lambda x,y: x+y).filter(lambda (id, links): id != -1)
    pagelinks_by_target = pagelinks_by_target.map(lambda (id, links): (id, remove_invalid_links(links)))

    pagelinks_by_source = pagelinks_by_target.flatMap(lambda x: [(src,[x[0]]) for src in x[1]]).reduceByKey(lambda x,y: x+y).filter(lambda (id, links): id != -1)
    pagelinks_by_source = pagelinks_by_source.map(lambda (id, links): (id, remove_invalid_links(links)) )


    pagerank = PageRankCalculator(pagelinks_by_source, pagelinks_by_target, sc)

    iterations = 1
    for i in range(iterations):
        pagerank.run_page_rank(0.85)

    sc.parallelize(pagerank.hard_scores.items()).saveAsTextFile(
        path="s3n://agiegerich-lda/pagelinks_output-full1/",
        compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec" 
    )
