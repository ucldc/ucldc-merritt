#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import logging
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

    parser = argparse.ArgumentParser(description='refresh merritt atom feeds already stashed on S3')
    parser.add_argument("id", help="registry collection id")
    parser.add_argument("--pynuxrc", help="rc file for use by pynux")
    parser.add_argument("--bucket", help="S3 bucket where feed is stashed")
    parser.add_argument("--dir", help="local directory where feed is written" )
    parser.add_argument("--nostash", action='store_true', help="write feed to local directory and do not stash on S3")

    argv = parser.parse_args()

    collection_id = argv.id

    kwargs = {}
    if argv.pynuxrc:
        kwargs['pynuxrc'] = argv.pynuxrc
    if argv.bucket:
        kwargs['bucket'] = argv.bucket
    if argv.dir:
        kwargs['dir'] = argv.dir
    if argv.nostash:
        kwargs['notstash'] = argv.nostash

    print "collection_id: {}".format(collection_id)
    print "kwargs: {}".format(kwargs)

    ma = MerrittAtom(collection_id, **kwargs)
    ma.process_feed()

if __name__ == "__main__":
    sys.exit(main())
