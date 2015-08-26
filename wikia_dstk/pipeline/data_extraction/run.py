"""
Responsible for handling the event stream - iterates over files in the
data_events S3 bucket and calls a set of services on each pageid/XML file
listed in order to warm the cache.
"""

import re
from boto import connect_s3
from boto.ec2 import connect_to_region
from boto.utils import get_instance_metadata
from time import sleep
from config import default_config
from ... import get_argparser_from_config, argstring_from_namespace
from child import child


def get_args():
    ap = get_argparser_from_config(default_config)
    ap.add_argument('--no-shutdown', dest='do_shutdown', action='store_false',
                    default=True)
    return ap.parse_known_args()


def run():
    args, extras = get_args()
    bucket = connect_s3().get_bucket('nlp-data')

    counter = 0
    while True:
        keys = [key.name for key in bucket.list(prefix='%s/' % args.queue) if
                re.sub(r'/?%s/?' % args.queue, '', key.name) is not '']
        print len(keys), "keys"
        while len(keys) > 0:
            k = keys.pop()
            child.apply_async(k)
            counter = 0
            sleep(5)
        counter += 1
        print 'No more keys, waiting 15 seconds. Counter: %d/20' % counter
        if args.do_shutdown and counter >= 20:
            print 'Scaling down, shutting down.'
            current_id = get_instance_metadata()['instance-id']
            ec2_conn = connect_to_region(args.region)
            ec2_conn.terminate_instances([current_id])
        sleep(15)
