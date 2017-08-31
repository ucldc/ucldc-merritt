#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import logging
import boto3
import argparse
from merritt_atom import MerrittAtom

def main():

    numeric_level = getattr(logging, 'INFO', None)
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s (%(name)s) [%(levelname)s]: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        stream=sys.stderr
    )
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description='Create ATOM feed for a given Nuxeo folder for Merritt harvesting')
    parser.add_argument("--pynuxrc", help="rc file for use by pynux")
    parser.add_argument("--bucket", help="S3 bucket where feed is stashed")
    parser.add_argument("--dir", help="local directory where feed is written" )
    parser.add_argument("--nostash", action='store_true', help="write feed to local directory and do not stash on S3")

    argv = parser.parse_args()

    kwargs = {}
    if argv.pynuxrc:
        kwargs['pynuxrc'] = argv.pynuxrc
    if argv.bucket:
        kwargs['bucket'] = argv.bucket
    if argv.dir:
        kwargs['dir'] = argv.dir

    # get a list of current feeds on S3
    bucketbase = 'static.ucldc.cdlib.org'
    prefix = 'merritt/'
 
    s3 = boto3.resource('s3')

    bucket = s3.Bucket(bucketbase)

    for obj in bucket.objects.filter(Prefix=prefix):
        if obj.key.endswith('.atom'):
            # get collection ID for each existing ATOM file
            filename = obj.key.split(prefix)[1]
            basename = filename.split('.')[0]
            collection_id = basename.split('_')[-1]
            logger.info("collection_id: {}".format(collection_id))

            # create and stash new feed for each
            ma = MerrittAtom(collection_id, **kwargs)
            ma.process_feed()

if __name__ == "__main__":
    sys.exit(main())
