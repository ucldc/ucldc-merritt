#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import argparse
from lxml import etree
from pynux import utils
from datetime import datetime
import dateutil.tz
import pprint
import urlparse
from deepharvest.deepharvest_nuxeo import DeepHarvestNuxeo
from os.path import expanduser
import codecs
import json
import requests
import boto
import logging

""" Given the Nuxeo document path for a collection folder, publish ATOM feed for objects for Merritt harvesting. """
pp = pprint.PrettyPrinter()
ATOM_NS = "http://www.w3.org/2005/Atom"
DC_NS = "http://purl.org/dc/elements/1.1/"
NX_NS = "http://www.nuxeo.org/ecm/project/schemas/tingle-california-digita/ucldc_schema"
NS_MAP = {None: ATOM_NS,
          "nx": NX_NS,
          "dc": DC_NS}
# we want to implement this mapping in the Registry:
MERRITT_ID_MAP = {'asset-library/UCM/Ramicova': 'ark:/13030/m5b58sn8',
                  'asset-library/UCM/Don Pedro Dam': 'ark:/13030/m5p60962',
                  'asset-library/UCM/MercedLocalHistoryCollection': 'ark:/13030/m5wm1bf0',
                  'asset-library/UCM/Assembly Newsletters': 'ark:/13030/m58h37qg',
                  'asset-library/UCSF/School_of_Dentistry_130': 'ark:/13030/m5xp9hp7',
                  'asset-library/UCSF/A_History_of_UCSF': 'ark:/13030/m5sx8rx2',
                  'asset-library/UCSF/30th_General_Hospital': 'ark:/13030/m5p58150',
                  'asset-library/UCSF/Day_Robert_L_Collection': 'ark:/13030/m5dn6hr0',
                  'asset-library/UCSF/Photograph_collection': 'ark:/13030/m5jd78gq',
                  'asset-library/UCSF/JapaneseWoodblocks': 'ark:/13030/m58w5s1p',
                  'asset-library/UCB/UCB\ EDA': 'ark:/13030/m500292r',
                  'asset-library/UCR': 'ark:/13030/m5qg11t8',
                  'asset-library/UCSC': 'ark:/13030/m5kq0912',
                  'asset-library/UCI/artists_books': 'ark:/13030/m56d5r8z'}
REGISTRY_API_BASE = 'https://registry.cdlib.org/api/v1/'
BUCKET = 'static.ucldc.cdlib.org/merritt' # FIXME put this in a conf file
FEED_BASE_URL = 'https://s3.amazonaws.com/{}/'.format(BUCKET)
'''
# following is mapping from Adrian. All are in Nuxeo except for UCSF Library Legacy Tobacco Documents Library
ark:/13030/m5b58sn8    University of California, Merced Library Nuxeo collections
ark:/13030/m52c19rr      UCSF Library Legacy Tobacco Documents Library
ark:/13030/m5xp9hp7   UCSF Library School of Dentistry 130th Anniversary
ark:/13030/m5sx8rx2     UCSF Library A History of UCSF
ark:/13030/m5p58150    UCSF Library 30th General Hospital
ark:/13030/m5dn6hr0    UCSF Library Robert L. Day Image Collection
ark:/13030/m5jd78gq     UCSF Library Photograph Collection
ark:/13030/m58w5s1p   UCSF Library Japanese Woodblock Print Collection
ark:/13030/m500292r     UC Berkeley Environmental Design Archives Nuxeo collections
ark:/13030/m5qg11t8     UC Riverside Nuxeo collections
ark:/13030/m5kq0912    UC Santa Cruz Nuxeo collections
'''

class MerrittAtom():

    def __init__(self, collection_id, pynuxrc=''):

        self.logger = logging.getLogger(__name__)

        self.collection_id = collection_id
        self.path = self._get_nuxeo_path()
        self.merritt_id = self.get_merritt_id(self.path)

        if pynuxrc:
            self.nx = utils.Nuxeo(rcfile=open(pynuxrc,'r'))
            self.dh = DeepHarvestNuxeo(self.path, '', pynuxrc=pynuxrc)
        elif not(pynuxrc) and os.path.isfile(expanduser('~/.pynuxrc')):
            self.nx = utils.Nuxeo(rcfile=open(expanduser('~/.pynuxrc'),'r'))
            self.dh = DeepHarvestNuxeo(self.path, '')

        self.atom_file = self._get_filename(self.collection_id)
        if not self.atom_file:
            raise ValueError("Could not create filename for ATOM feed based on collection id: {}".format(self.collection_id))

        self.s3_url = "{}{}".format(FEED_BASE_URL, self.atom_file)

    def get_merritt_id(self, path):
        ''' given the Nuxeo path, get corresponding Merritt collection ID '''
        merritt_id = None
        path = path.lstrip('/')
        fullpath = path
        while len(path.split('/')) > 1:
            if path in MERRITT_ID_MAP:
                merritt_id = MERRITT_ID_MAP[path]
            path = os.path.dirname(path)
        if merritt_id is None:
            raise KeyError("Could not find match for '{}' in MERRITT_ID_MAP".format(fullpath))
        return merritt_id 

    def _get_nuxeo_path(self):
        ''' given ucldc registry collection ID, get Nuxeo path for collection '''
        url = "{}collection/{}/?format=json".format(REGISTRY_API_BASE, self.collection_id)
        res = requests.get(url)
        res.raise_for_status()
        md = json.loads(res.text)
        nuxeo_path = md['harvest_extra_data']

        return nuxeo_path 

    def _get_filename(self, collection_id):
        ''' given Collection ID, get a friendly filename for the ATOM feed '''
        filename = 'ucldc_collection_{}.atom'.format(collection_id)

        return filename 

    def _extract_nx_metadata(self, uid): 
        ''' extract Nuxeo metadata we want to post to the ATOM feed '''
        raw_metadata = self.nx.get_metadata(uid=uid)
        metadata = {}
        
        # creator
        creators = raw_metadata['properties']['ucldc_schema:creator']
        metadata['creator'] = [creator['name'] for creator in creators]

        # title
        metadata['title'] = raw_metadata['title']

        # date
        dates = raw_metadata['properties']['ucldc_schema:date']
        dates = [date['date'] for date in dates]
        metadata['date'] = dates[0] if dates else None

        # nuxeo id
        metadata['id'] = raw_metadata['properties']['ucldc_schema:identifier']

        # nuxeo collection
        metadata['collection'] = raw_metadata['properties']['ucldc_schema:collection'][0] if raw_metadata['properties']['ucldc_schema:collection'] else None

        return metadata

    def _construct_entry(self, uid, is_parent):
        ''' construct ATOM feed entry element for a given nuxeo doc '''
        nx_metadata = self._extract_nx_metadata(uid)
        entry = etree.Element(etree.QName(ATOM_NS, "entry"))
        entry = self._populate_entry(entry, nx_metadata, uid, is_parent)

        return entry

    def _construct_entry_bundled(self, doc):
        ''' construct ATOM feed entry element for a given nuxeo doc, including files for any component objects '''
        # parent
        uid = doc['uid']
        nx_metadata = self._extract_nx_metadata(uid)
        entry = etree.Element(etree.QName(ATOM_NS, "entry"))
        entry = self._populate_entry(entry, nx_metadata, uid, True)

        # insert component md
        for c in self.dh.fetch_components(doc):
            self._insert_full_md_link(entry, c['uid'])
            self._insert_main_content_link(entry, c['uid'])
            self._insert_aux_links(entry, c['uid'])

        return entry

    def _add_atom_elements(self, doc):
        ''' add atom feed elements to document '''

        # recommended ATOM feed elements
        feed_author = etree.Element(etree.QName(ATOM_NS, "author"))
        feed_author.text = "UC Libraries Digital Collection"
        doc.insert(0, feed_author)

        # required ATOM feed elements
        feed_title = etree.Element(etree.QName(ATOM_NS, "title"))
        feed_title.text = "UCLDC Metadata Feed" # FIXME get campus name from registry API?
        doc.insert(0, feed_title)

        feed_id = etree.Element(etree.QName(ATOM_NS, "id"))
        feed_id.text = self.s3_url 
        doc.insert(0, feed_id)

        return doc 

    def _add_feed_updated(self, doc, updated):
        ''' add feed updated '''
        feed_updated = etree.Element(etree.QName(ATOM_NS, "updated"))
        feed_updated.text = updated 
        doc.insert(0, feed_updated)

    def _add_collection_alt_link(self, doc, path):
        ''' add elements related to Nuxeo collection info to document '''
        collection_metadata = self.nx.get_metadata(path=path)
        collection_title = collection_metadata['title']
        collection_uid = collection_metadata['uid']
        collection_uri = self.get_object_view_url(collection_uid)

        feed_link_alt = etree.Element(etree.QName(ATOM_NS, "link"), rel="alternate", href=collection_uri, title=collection_title) 
        doc.insert(0, feed_link_alt)

        return doc

    def _add_paging_info(self, doc):
        ''' add rel links for paging '''
        # this is just dumb for now
        last_link = etree.Element(etree.QName(ATOM_NS, "link"), rel="last", href=self.s3_url)
        doc.insert(0, last_link)

        first_link = etree.Element(etree.QName(ATOM_NS, "link"), rel="first", href=self.s3_url)
        doc.insert(0, first_link)

        self_link = etree.Element(etree.QName(ATOM_NS, "link"), rel="self", href=self.s3_url)
        doc.insert(0, self_link)

    def _add_merritt_id(self, doc, merritt_collection_id):
        ''' add Merritt ID '''
        merritt_id = etree.Element(etree.QName(ATOM_NS, "merritt_collection_id"))
        merritt_id.text = merritt_collection_id 
        doc.insert(0, merritt_id)

    def _populate_entry(self, entry, metadata, nxid, is_parent):
        ''' get <entry> element for a given set of object metadata '''

        # atom id (URI)
        nuxeo_object_view_url = self.get_object_view_url(nxid)
        atom_id = etree.SubElement(entry, etree.QName(ATOM_NS, "id"))
        atom_id.text = nuxeo_object_view_url

        # atom title
        atom_title = etree.SubElement(entry, etree.QName(ATOM_NS, "title"))
        atom_title.text = metadata["title"]
 
        # atom updated
        atom_updated = etree.SubElement(entry, etree.QName(ATOM_NS, "updated"))
        atom_updated.text = datetime.now(dateutil.tz.tzutc()).isoformat()
        self.last_update = atom_updated.text

        # atom author
        atom_author = etree.SubElement(entry, etree.QName(ATOM_NS, "author"))
        atom_author.text = "UC Libraries Digital Collection"

        # metadata file link
        self._insert_full_md_link(entry, nxid)

        # media json link
        if is_parent:
            self._insert_media_json_link(entry, nxid)

        # main content file link
        self._insert_main_content_link(entry, nxid)

        # auxiliary file link(s)
        self._insert_aux_links(entry, nxid)

        # dc creator
        for creator_name in metadata['creator']:
            dc_creator = etree.SubElement(entry, etree.QName(DC_NS, "creator"))
            dc_creator.text = creator_name 

        # dc title
        dc_title = etree.SubElement(entry, etree.QName(DC_NS, "title"))
        dc_title.text = metadata['title']

        # dc date
        dc_date = etree.SubElement(entry, etree.QName(DC_NS, "date"))
        dc_date.text = metadata['date']

        # dc identifier (a.k.a. local identifier) - Nuxeo ID
        nuxeo_identifier = etree.SubElement(entry, etree.QName(DC_NS, "identifier"))
        nuxeo_identifier.text = nxid

        # UCLDC identifier (a.k.a. local identifier) - ucldc_schema:identifier -- this will be the ARK if we have it
        if metadata['id']:
            ucldc_identifier = etree.SubElement(entry, etree.QName(NX_NS, "identifier"))
            ucldc_identifier.text = metadata['id']

        # UCLDC collection identifier
        ucldc_collection_id = etree.SubElement(entry, etree.QName(NX_NS, "collection"))
        ucldc_collection_id.text = metadata['collection']

        return entry

    def _insert_media_json_link(self, entry, uid):
        media_json_url = self.get_media_json_url(uid)
        link_media_json = etree.SubElement(entry, etree.QName(ATOM_NS, "link"), rel="alternate", href=media_json_url, type="application/json", title="Deep Harvest metadata for this object") 


    def _insert_main_content_link(self, entry, uid):
        nx_metadata = self.nx.get_metadata(uid=uid)
        nuxeo_file_download_url = self.get_object_download_url(nx_metadata)
        main_content_link = etree.SubElement(entry, etree.QName(ATOM_NS, "link"), rel="alternate", href=nuxeo_file_download_url, title="Main content file") # FIXME add content_type


    def _insert_aux_links(self, entry, uid):
        nx_metadata = self.nx.get_metadata(uid=uid)
        aux_file_urls = self.get_aux_file_urls(nx_metadata)
        for af in aux_file_urls:
            link_aux_file = etree.SubElement(entry, etree.QName(ATOM_NS, "link"), rel="alternate", href=af, title="Auxiliary file")


    def _insert_full_md_link(self, entry, uid):
        full_metadata_url = self.get_full_metadata(uid)
        link_md = etree.SubElement(entry, etree.QName(ATOM_NS, "link"), rel="alternate", href=full_metadata_url, type="application/xml", title="Full metadata for this object from Nuxeo")


    def _write_feed(self, doc):
        ''' publish feed '''
        feed = etree.ElementTree(doc)
        feed_string = etree.tostring(feed, pretty_print=True, encoding='utf-8', xml_declaration=True)

        with open(self.atom_file, "w") as f:
            f.write(feed_string)
      
    def _s3_stash(self):
       """ Stash file in S3 bucket. 
       """
       s3_url = 's3://{}/{}'.format(BUCKET, self.atom_file)
       bucketpath = BUCKET.strip("/")
       bucketbase = BUCKET.split("/")[0]
       parts = urlparse.urlsplit(s3_url)
       mimetype = 'application/xml' 
       
       conn = boto.connect_s3()

       try:
           bucket = conn.get_bucket(bucketbase)
       except boto.exception.S3ResponseError:
           bucket = conn.create_bucket(bucketbase)
           self.logger.info("Created S3 bucket {}".format(bucketbase))

       if not(bucket.get_key(parts.path)):
           key = bucket.new_key(parts.path)
           key.set_metadata("Content-Type", mimetype)
           key.set_contents_from_filename(self.atom_file)
           msg = "created {0}".format(s3_url)
           self.logger.info(msg)
       else:
           key = bucket.get_key(parts.path)
           key.set_metadata("Content-Type", mimetype)
           key.set_contents_from_filename(self.atom_file)
           msg = "re-uploaded {}".format(s3_url)
           self.logger.info(msg)

    def get_object_view_url(self, nuxeo_id):
        """ Get object view URL """
        parts = urlparse.urlsplit(self.nx.conf["api"])
        url = "{}://{}/Nuxeo/nxdoc/default/{}/view_documents".format(parts.scheme, parts.netloc, nuxeo_id) 
        return url

    def get_full_metadata(self, nuxeo_id):
        """ Get full metadata via Nuxeo API """
        parts = urlparse.urlsplit(self.nx.conf["api"])
        url = '{}://{}/Merritt/{}.xml'.format(parts.scheme, parts.netloc, nuxeo_id)
    
        return url

    def get_object_download_url(self, metadata):
        ''' given the full metadata for an object, get file download url '''
        try:
            file_content = metadata['properties']['file:content']
        except KeyError:
            raise KeyError("Nuxeo object metadata does not contain 'properties/file:content' element. Make sure 'X-NXDocumentProperties' provided in pynux conf includes 'file'")

        if file_content is None:
            return None
        else:
            url = file_content['data']

        # make available via basic auth
        url = url.replace('/nuxeo/', '/Nuxeo/')
     
        return url

    def get_media_json_url(self, nuxeo_id):
        """ Get media.json (deep harvest) url """
        # https://s3.amazonaws.com/static.ucldc.cdlib.org/media_json/002130a5-e171-461b-a41b-28ab46af9652-media.json
        url = "https://s3.amazonaws.com/static.ucldc.cdlib.org/media_json/{}-media.json".format(nuxeo_id)

        return url

    def get_aux_file_urls(self, metadata):
        ''' get auxiliary file urls '''
        urls = []
        
        # get any "attachment" files
        if metadata['properties']['files:files']:
            attachments = metadata['properties']['files:files']
            for attachment in attachments:
                url = attachment['file']['data']
                url = url.replace('/nuxeo/', '/Nuxeo/')
                urls.append(url) 

        # get any "extra_file" files
        if metadata['properties']['extra_files:file']:
            for extra_file in metadata['properties']['extra_files:file']:
                url = extra_file['blob']['data']
                url = url.replace('/nuxeo/', '/Nuxeo/')
                urls.append(url)

        return urls 

def main(argv=None):
    parser = argparse.ArgumentParser(description='Create ATOM feed for a given Nuxeo folder for Merritt harvesting')
    parser.add_argument("collection", help="UCLDC Registry Collection ID")
    parser.add_argument("--pynuxrc", help="rc file for use by pynux")
    if argv is None:
        argv = parser.parse_args()
    collection_id = argv.collection

    if argv.pynuxrc:
        ma = MerrittAtom(collection_id, argv.pynuxrc)
    else:
        ma = MerrittAtom(collection_id)

    print "atom_file: {}".format(ma.atom_file)
    print "ma.path: {}".format(ma.path)

    print "Nuxeo path: {}".format(ma.path)
    print "Fetching Nuxeo docs. This could take a while if collection is large..."
    documents = ma.dh.fetch_objects()

    # create root
    root = etree.Element(etree.QName(ATOM_NS, "feed"), nsmap=NS_MAP)

    # add entries
    for document in documents:
        nxid = document['uid']
        print "working on document: {} {}".format(nxid, document['path'])

        # object, bundled into one <entry> if complex
        entry = ma._construct_entry_bundled(document)
        print "inserting entry for object {} {}".format(nxid, document['path'])
        root.insert(0, entry)

        '''
        # parent
        entry = ma._construct_entry(nxid, True)
        print "inserting entry for parent object {} {}".format(nxid, document['path'])
        root.insert(0, entry)

        # children
        component_entries = [ma._construct_entry(c['uid'], False) for c in dh.fetch_components(document)]
        for ce in component_entries:
            print "inserting entry for component: {} {}".format(nxid, document['path'])
            root.insert(0, ce)
        '''

    # add header info
    print "Adding header info to xml tree"
    ma._add_merritt_id(root, ma.merritt_id)
    ma._add_paging_info(root)
    ma._add_collection_alt_link(root, ma.path)
    ma._add_atom_elements(root)
    ma._add_feed_updated(root, ma.last_update)

    ma._write_feed(root)
    print "Feed written to file: {}".format(ma.atom_file)

    ma._s3_stash()
    print "Feed stashed on s3: {}".format(ma.s3_url)

if __name__ == "__main__":
    sys.exit(main())
