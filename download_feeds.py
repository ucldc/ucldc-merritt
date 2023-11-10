#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import boto3

bucketname = 'static.ucldc.cdlib.org'
prefix = 'merritt/'

s3 = boto3.resource('s3')
bucket = s3.Bucket(bucketname)

for obj in bucket.objects.filter(Prefix=prefix):
    if obj.key.endswith('.atom'):
        print("downloading {}".format(obj.key))
        filename = obj.key.split('/')[1] 
        filepath = './feeds_current/{}'.format(filename)
        print("local filepath: {}".format(filepath))
        s3.Bucket('static.ucldc.cdlib.org').download_file(obj.key, filepath)
