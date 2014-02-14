"""
Responsible for handling the event stream - iterates over files in the data_events S3 bucket and calls a set of services on each pageid/XML file listed in order to warm the cache.
"""

import re
import sys
from ..config.data_extraction import config
from boto import connect_s3
from boto.ec2 import connect_to_region
from boto.utils import get_instance_metadata
from subprocess import Popen
from time import sleep

workers = int(sys.argv[1])
BUCKET = connect_s3().get_bucket('nlp-data')

counter = 0
while True:
    processes = []
    keys = [key.name for key in BUCKET.list(prefix='data_events/') if re.sub(r'/?data_events/?', '', key.name) is not '']
    while len(keys) > 0:
        counter = 0
        while len(processes) < workers:
            processes.append(Popen('/home/ubuntu/venv/bin/python -m wikia_dstk.pipeline.data_extraction.child %s' % keys.pop(), shell=True))
        processes = filter(lambda x: x.poll() is None, processes)
        sleep(0.25)
    counter += 1
    print 'No more keys, waiting 15 seconds. Counter: %d/20' % counter
    if counter >= 20:
        print 'Scaling down, shutting down.'
        current_id = get_instance_metadata()['instance-id']
        ec2_conn = connect_to_region(config['region'])
        ec2_conn.terminate_instances([current_id])
    sleep(15)