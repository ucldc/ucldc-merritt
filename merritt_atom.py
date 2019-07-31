#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
from lxml import etree
from pynux import utils
from datetime import datetime
import dateutil.tz
from dateutil.parser import parse
import urlparse
from deepharvest.deepharvest_nuxeo import DeepHarvestNuxeo
from os.path import expanduser
import codecs
import json
import requests
import boto3
import logging
from operator import itemgetter
import collections

""" Given the Nuxeo document path for a collection folder, publish ATOM feed for objects for Merritt harvesting. """
ATOM_NS = "http://www.w3.org/2005/Atom"
DC_NS = "http://purl.org/dc/elements/1.1/"
NX_NS = "http://www.nuxeo.org/ecm/project/schemas/tingle-california-digita/ucldc_schema"
OPENSEARCH_NS = "http://a9.com/-/spec/opensearch/1.1/"
NS_MAP = {None: ATOM_NS,
          "nx": NX_NS,
          "dc": DC_NS,
          "opensearch": OPENSEARCH_NS}
REGISTRY_API_BASE = 'https://registry.cdlib.org/api/v1/'
BUCKET = 'static.ucldc.cdlib.org/merritt'

class MerrittAtom():

    def __init__(self, collection_id, **kwargs):

        self.logger = logging.getLogger(__name__)

        self.collection_id = collection_id

        if 'bucket' in kwargs:
            self.bucket = kwargs['bucket']
        else:
            self.bucket = BUCKET

        if 'pynuxrc' in kwargs:
            pynuxrc = kwargs['pynuxrc']
        else:
            pynuxrc = None

        if 'dir' in kwargs:
            self.dir = kwargs['dir']
        else:
            self.dir = '.'

        if 'nostash' in kwargs:
            self.nostash = kwargs['nostash']
        else:
            self.nostash = False

        self.logger.info("collection_id: {}".format(self.collection_id))

        if 'nuxeo_path' in kwargs:
            self.path = kwargs['nuxeo_path']
        else:
            self.path = self._get_nuxeo_path()

        if 'merritt_id' in kwargs:
            self.merritt_id = kwargs['merritt_id']
        else:
            self.merritt_id = self._get_merritt_id()

        if not self.merritt_id:
            raise ValueError("No Merritt ID for this collection")

        self.feed_base_url = 'https://s3.amazonaws.com/{}/'.format(self.bucket)

        if pynuxrc:
            self.nx = utils.Nuxeo(rcfile=open(expanduser(pynuxrc),'r'))
            self.dh = DeepHarvestNuxeo(self.path, '', pynuxrc=pynuxrc)
        elif not(pynuxrc) and os.path.isfile(expanduser('~/.pynuxrc')):
            self.nx = utils.Nuxeo(rcfile=open(expanduser('~/.pynuxrc'),'r'))
            self.dh = DeepHarvestNuxeo(self.path, '')

        self.atom_file = self._get_filename(self.collection_id)
        if not self.atom_file:
            raise ValueError("Could not create filename for ATOM feed based on collection id: {}".format(self.collection_id))

        self.s3_url = "{}{}".format(self.feed_base_url, self.atom_file)

        self.atom_filepath = os.path.join(self.dir, self.atom_file)

    def _get_merritt_id(self):
        ''' given collection registry ID, get corresponding Merritt collection ID '''
        url = "{}collection/{}/?format=json".format(REGISTRY_API_BASE, self.collection_id)
        res = requests.get(url)
        res.raise_for_status()
        md = json.loads(res.text)
        merritt_id = md['merritt_id']

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

    def _extract_nx_metadata(self, raw_metadata): 
        ''' extract Nuxeo metadata we want to post to the ATOM feed '''
        metadata = {}
        
        # last modified 
        metadata['lastModified'] = raw_metadata['bundle_lastModified']

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

    def _construct_entry_bundled(self, doc):
        ''' construct ATOM feed entry element for a given nuxeo doc, including files for any component objects '''
        uid = doc['uid']

        # parent
        nx_metadata = self._extract_nx_metadata(doc)
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
        atom_updated.text = metadata['lastModified'].isoformat()

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
        checksum = self.get_nuxeo_file_checksum(nx_metadata)
        if nuxeo_file_download_url:
            main_content_link = etree.SubElement(entry, etree.QName(ATOM_NS, "link"), rel="alternate", href=nuxeo_file_download_url, title="Main content file") # FIXME add content_type
        
        if checksum:
            checksum_element = etree.SubElement(main_content_link, etree.QName(OPENSEARCH_NS, "checksum"), algorithm="MD5")
            checksum_element.text = checksum

    def _insert_aux_links(self, entry, uid):
        nx_metadata = self.nx.get_metadata(uid=uid)
        aux_files = self.get_aux_files(nx_metadata)
        for af in aux_files:
            link_aux_file = etree.SubElement(entry, etree.QName(ATOM_NS, "link"), rel="alternate", href=af['url'], title="Auxiliary file")
            if af['checksum']:
                checksum_element = etree.SubElement(link_aux_file, etree.QName(OPENSEARCH_NS, "checksum"), algorithm="MD5")
                checksum_element.text = af['checksum']

    def _insert_full_md_link(self, entry, uid):
        full_metadata_url = self.get_full_metadata(uid)
        link_md = etree.SubElement(entry, etree.QName(ATOM_NS, "link"), rel="alternate", href=full_metadata_url, type="application/xml", title="Full metadata for this object from Nuxeo")


    def _write_feed(self, doc):
        ''' publish feed '''
        feed = etree.ElementTree(doc)
        feed_string = etree.tostring(feed, pretty_print=True, encoding='utf-8', xml_declaration=True)

        with open(self.atom_filepath, "w") as f:
            f.write(feed_string)
      
    def _s3_get_feed(self):
       """ Retrieve ATOM feed file from S3. Return as ElementTree object """
       bucketpath = self.bucket.strip("/")
       bucketbase = self.bucket.split("/")[0]
       keyparts = bucketpath.split("/")[1:]
       keyparts.append(self.atom_file)
       keypath = '/'.join(keyparts)

       s3 = boto3.client('s3')
       response = s3.get_object(Bucket=bucketbase,Key=keypath)
       contents = response['Body'].read()

       return etree.fromstring(contents) 

    def _s3_stash(self):
       """ Stash file in S3 bucket.
       """
       bucketpath = self.bucket.strip("/")
       bucketbase = self.bucket.split("/")[0]
       keyparts = bucketpath.split("/")[1:]
       keyparts.append(self.atom_file)
       keypath = '/'.join(keyparts)

       s3 = boto3.client('s3')
       with open(self.atom_filepath, 'r') as f:
           s3.upload_fileobj(f, bucketbase, keypath)

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

    def get_nuxeo_file_checksum(self, metadata):
        ''' get md5 checksum for nuxeo file '''
        try:
            file_content = metadata['properties']['file:content']
        except KeyError:
            raise KeyError("Nuxeo object metadata does not contain 'properties/file:content' element. Make sure 'X-NXDocumentProperties' provided in pynux conf includes 'file'")

        if file_content is None:
            return None
        else:
            checksum = file_content['digest']

        return checksum

    def get_aux_files(self, metadata):
        ''' get auxiliary file urls '''
        all_md = []
        
        # get any "attachment" files
        if metadata['properties']['files:files']:
            attachments = metadata['properties']['files:files']
            for attachment in attachments:
                md = {}
                if attachment['file'] and attachment['file']['data']:
                    url = attachment['file']['data']
                    url = url.replace('/nuxeo/', '/Nuxeo/')
                    md['url'] = url
                if attachment['file'] and attachment['file']['digest']:
                    md['checksum'] = attachment['file']['digest']
                if md:
                    all_md.append(md)

        # get any "extra_file" files
        if metadata['properties']['extra_files:file']:
            for extra_file in metadata['properties']['extra_files:file']:
                md = {}
                if extra_file['blob'] and extra_file['blob']['data']:
                    url = extra_file['blob']['data']
                    url = url.replace('/nuxeo/', '/Nuxeo/')
                    md['url'] = url
                if extra_file['blob'] and extra_file['blob']['digest']:    
                    md['checksum'] = extra_file['blob']['digest']
                if md:
                    all_md.append(md)

        return all_md 

    def _bundle_docs(self, docs):
        ''' given a dict of parent level nuxeo docs, fetch any components
            and also figure out when any part of the object was most 
            recently modified/added '''

        for doc in docs:

            last_mod_str = doc['lastModified']
            overall_mod_datetime = parse(last_mod_str)

            doc['components'] = []
            
            for c in doc['components']:
                mod_str = c['lastModified']
                mod_datetime = parse(mod_str)
        
                if mod_datetime > overall_mod_datetime:
                    overall_mod_datetime = mod_datetime 

            doc['bundle_lastModified'] = overall_mod_datetime

        return docs 

    def has_duplicates(self, root):
        ''' check to see if there are duplicate entries in the feed '''
        feed = root.iterfind('{http://www.w3.org/2005/Atom}feed/')
        ids = root.iter("{http://purl.org/dc/elements/1.1/}identifier")

        count = 0
        idlist = []
        for identifier in ids:
            count = count + 1
            #print(identifier.text), count
            idlist.append(identifier.text)

        dups = [item for item, count in collections.Counter(idlist).items() if count > 1]

        if len(dups) > 0:
            return True
        else:
            return False


    def process_feed(self):
        ''' create feed for collection and stash on s3 '''
        self.logger.info("atom_file: {}".format(self.atom_file))
        self.logger.info("Nuxeo path: {}".format(self.path))
        self.logger.info("Fetching Nuxeo docs. This could take a while if collection is large...")

        parent_docs = self.dh.fetch_objects()

        bundled_docs = self._bundle_docs(parent_docs)
        bundled_docs.sort(key=itemgetter('bundle_lastModified'))

        # create root
        root = etree.Element(etree.QName(ATOM_NS, "feed"), nsmap=NS_MAP)

        # add entries
        for document in bundled_docs:
            nxid = document['uid']
            self.logger.info("working on document: {} {}".format(nxid, document['path']))

            # object, bundled into one <entry> if complex
            entry = self._construct_entry_bundled(document)
            self.logger.info("inserting entry for object {} {}".format(nxid, document['path']))
            root.insert(0, entry)

        # add header info
        logging.info("Adding header info to xml tree")
        self._add_merritt_id(root, self.merritt_id)
        self._add_paging_info(root)
        self._add_collection_alt_link(root, self.path)
        self._add_atom_elements(root)
        self._add_feed_updated(root, datetime.now(dateutil.tz.tzutc()).isoformat())

        self._write_feed(root)
        logging.info("Feed written to file: {}".format(self.atom_filepath))

        if self.has_duplicates(root):
            self.logger.warning("Duplicates in feed {}. Will not stash on S3.".format(self.atom_filepath))
            return 'DUPS'

        if not self.nostash:
            self._s3_stash()
            self.logger.info("Feed stashed on s3: {}".format(self.s3_url)) 

        return 'OK'

def main(argv=None):
    pass

if __name__ == "__main__":
    sys.exit(main())
