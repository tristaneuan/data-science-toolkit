from __future__ import division
import requests
from .. import EC2Connection
#from ..top5k import top5k
from collections import defaultdict
from optparse import OptionParser, OptionError
from time import sleep

from config import config

LAST_INDEXED = '/data/last_indexed.txt'
SOLR = 'http://search-s10:8983/solr/main/select'

op = OptionParser()
op.add_option('-d', '--date', dest='date', help='The query start date')
op.add_option('-r', '--region', dest='region', help='The EC2 region to connect to')
op.add_option('-c', '--cost', dest='price', help='The maximum bid price')
op.add_option('-a', '--ami', dest='ami', help='The AMI to use')
op.add_option('-k', '--key', dest='key', help='The name of the key pair')
op.add_option('-s', '--security-groups', dest='sec',
              help='The security groups with which to associate instances')
op.add_option('-i', '--instance-type', dest='type',
              help='The type of instance to run')
op.add_option('-t', '--tag', dest='tag',
              help='The name of the tag to operate over')
op.add_option('-m', '--max-size', dest='max_size', type='int',
              help='The maximum allowable number of simultaneous instances')
(options, args) = op.parse_args()

config.update([(k, v) for (k, v) in vars(options).items() if v is not None])

ec2_conn = EC2Connection(config)

# Read the last indexed date
date = options.date
if date is None:
    with open(LAST_INDEXED, 'r') as f:
        date = f.read().strip()

date = '2014-02-20T23:59:59.999' # DEBUG

print 'Last indexed date:', date

params = {
             'q': 'lang:en AND iscontent:true AND indexed:[%sZ TO NOW]' % date,
             'fl': 'wid,wikipages',
             'rows': '100', # 10000
             #'sort': 'wam_i desc',
             'facet': 'true',
             'facet.limit': '100', # -1
             #'facet.query': 'indexed:[%sZ TO NOW]' % date,
             'facet.field': 'wid',
             'wt': 'json'
         }

while True:
    try:
        r = requests.get(SOLR, params=params).json()
    except requests.exceptions.ConnectionError as e:
        print e
        print 'retrying...'
        continue
    break
docs = r['response']['docs']
# Populate dict - {wid: number of articles} (using wikipages)
print 'Populating article count dict...'
articles = dict((doc['wid'], doc.get('wikipages', 0)) for doc in docs)
#from pprint import pprint; pprint(articles)
# Populate dict - {wid: int indicating whether indexed since given date}
ids = r['facet_counts']['facet_fields']['wid']
print 'Populating indexed dict...'
d = dict((int(ids[i]), ids[i+1]) for i in range(0, len(ids), 2))
#pprint(d)

## Find wikis in top 5k indexed since given date
#print 'Filtering wiki IDs by indexed since last date...'
#wids = filter(lambda x: d.get(x), top5k)

# Split into groups of approx equal total doc count, args = {inst #: wids to pass}
print 'Splitting wiki IDs into approximately equal groups...'
total_articles = sum(articles.values())
parts = config['max_size']
quota = total_articles / parts
print 'PARTS: %d' % parts
print 'TOTAL ARTICLES: %d' % total_articles
print 'QUOTA: %d' % quota
#wids = sorted(wids, key=lambda x: articles.get(x, 0), reverse=True)
wids = sorted(filter(lambda x: d[x] > 0, d.keys()), key=lambda y: articles.get(y, 0), reverse=True)
print len(wids)
args = defaultdict(list)

bins = []
for wid in wids:
    count = articles.get(wid, 0)
    for bin in bins:
        if sum([articles.get(wid, 0) for wid in bin]) + count <= quota:
            bin.append(wid)
            break
    else:
        bin = []
        bin.append(wid)
        bins.append(bin)
for n, bin in enumerate(bins):
    print 'Part %d contains %d articles' % (n, sum([articles.get(wid, 0) for wid in bin]))

###part = 0
###count = 0
#for wid in wids:
###for n, wid in enumerate(wids):
###    if count >= quota:
###        print 'Part %d contains %d articles' % (part, count)
###        #print 'Index: %d' % n
###        count = 0
###        part += 1
###    args[part].append(wid)
###    count += articles.get(wid, 0)
###print 'Part %d contains %d articles' % (part, count)
####print 'Index: %d' % n
###print len(args.keys())
###import sys; sys.exit(0)

#from bins import packAndShow
#packAndShow(articles.values(), quota+1)
import sys; sys.exit(0)


for i in range(0, len(wids), parts):
    #print '%d/%d complete' % (i, len(wids))
    for n, wid in enumerate(wids[i:i+parts]):
        args[n].append(wid)

for n in args:
    #print '%d: %s' % (n, ','.join(args[n]))
    print 'Part %d contains %d articles' % (n, sum(args[n]))
import sys; sys.exit(0)

# Write individual user_data shell scripts with wids passed as args and launch
# EC2 instances to execute them one at a time
for n in args:
    user_data = """#!/bin/sh
    export WIKIS="%s"
    /home/ubuntu/venv/bin/python -m wikia_dstk.pipeline.wiki_data_extraction.run > /home/ubuntu/wiki_data_extraction.log
    """ % ','.join(args[n])
    print 'User data for instance #%d:\n%s' % (n+1, user_data)
    print 'Launching instance %d of %d' % (n+1, len(args))
    ec2_conn.add_instances(1, user_data)