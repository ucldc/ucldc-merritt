#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pynux import utils
from os.path import expanduser
import collections 
import pprint
pp = pprint.PrettyPrinter(indent=4)

CHILD_NXQL = "SELECT * FROM Document WHERE ecm:parentId = '{}' AND " \
             "ecm:isTrashed = 0 ORDER BY ecm:pos"

nx = utils.Nuxeo(rcfile=open(expanduser('~/.pynuxrc'), 'r'))

query = CHILD_NXQL.format('afa37e7d-051f-4fbc-a5b4-e83aa6171aef')
uids = []
for child in nx.nxql(query):
   uids.append(child['uid'])

#pp.pprint(uids)
count = len(uids)
dups = [item for item, count in list(collections.Counter(uids).items()) if count > 1]
#pp.pprint(dups)
print(len(uids))
print(len(dups))
