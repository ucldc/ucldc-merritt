#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import argparse
from lxml import etree
from pynux import utils
from datetime import datetime
import dateutil.tz
import pprint


""" Given the Nuxeo document path for a collection folder, publish ATOM feed for objects for Merritt harvesting. """
pp = pprint.PrettyPrinter()

def main(argv=None):

    parser = argparse.ArgumentParser(description='Create ATOM feed for a given Nuxeo folder for Merritt harvesting')
    parser.add_argument('path', nargs=1, help="Nuxeo document path")
    utils.get_common_options(parser)
    if argv is None:
        argv = parser.parse_args()

    nx = utils.Nuxeo(rcfile=argv.rcfile, loglevel=argv.loglevel.upper())
    nx_path = argv.path[0]

    # get metadata for top-level nuxeo document path (usually the path to a collection)
    collection_metadata = nx.get_metadata(path=nx_path)
    collection_title = collection_metadata['title']
    collection_uid = collection_metadata['uid'] 

    # create ATOM feed with multiple namespaces
    ATOM_NS = "http://www.w3.org/2005/Atom"
    DC_NS = "http://purl.org/dc/elements/1.1/"
    NX_NS = "http://www.nuxeo.org/ecm/project/schemas/tingle-california-digita/ucldc_schema"
    NS_MAP = {None: ATOM_NS,
              "nx": NX_NS,
              "dc": DC_NS}
    root = etree.Element("feed", nsmap=NS_MAP)
    feed = etree.ElementTree(root)

    # required ATOM feed elements
    feed_id = etree.SubElement(root, etree.QName(ATOM_NS, "id"))
    feed_id.text = "http://nuxeo.cdlib.org"

    feed_title = etree.SubElement(root, etree.QName(ATOM_NS, "title")) 
    feed_title.text = "UCXX UCLDC Metadata Feed" # FIXME get campus name from registry API?

    feed_updated = etree.SubElement(root, etree.QName(ATOM_NS, "updated")) 
    feed_updated.text = datetime.now(dateutil.tz.tzutc()).isoformat()    

    # recommended ATOM feed elements
    feed_author = etree.SubElement(root, etree.QName(ATOM_NS, "author"))
    feed_author.text = "UC Libraries Digital Collection"

    collection_uri = get_object_view_url(collection_uid)
    feed_link_alt = etree.SubElement(root, etree.QName(ATOM_NS, "link"), rel="alternate", href=collection_uri, title=collection_title)

    # get ids or paths for all documents in this collection.
    # Note: ultimately we will want to get info on complex object structure via the 'structMap' property of the -media.json file for each object. (see https://github.com/ucldc/ucldc-docs/wiki/media.json). For now this assumes simple objects only.
    documents = nx.children(nx_path)
    nxids = [document['uid'] for document in documents]

    # create ATOM feed entry for each object
    documents = nx.children(argv.path[0])
    nxids = [document['uid'] for document in documents]
    for nxid in nxids:

        # get metadata via Nuxeo REST API
        metadata = nx.get_metadata(uid=nxid)

        creators = metadata['properties']['ucldc_schema:creator']
        creator_names = [creator['name'] for creator in creators]

        title = metadata['title']

        dates = metadata['properties']['ucldc_schema:date']
        dates = [date['date'] for date in dates]
        date = dates[0] if dates else None

        ucldc_id = metadata['properties']['ucldc_schema:identifier']

        ucldc_collection = metadata['properties']['ucldc_schema:collection'][0] if metadata['properties']['ucldc_schema:collection'] else None

        # locations of various component objects and metadata
        full_metadata_url = get_full_metadata(nxid)
        media_json_url = get_structural_metadata(nxid)
        nuxeo_object_view_url = get_object_view_url(nxid) 
        nxpath = metadata['path']
        nuxeo_file_download_url = get_object_download_url(nxid, nxpath)
        #content_type = metadata['properties']['picture:views'][0]['content']['mime-type'] # FIXME

        # create <entry>
        entry = etree.SubElement(root, etree.QName(ATOM_NS, "entry"))

        ############################
        #  ATOM namespace elements
        ############################
        # atom id (URI)
        atom_id = etree.SubElement(entry, etree.QName(ATOM_NS, "id"))
        atom_id.text = nuxeo_object_view_url

        # atom title
        atom_title = etree.SubElement(entry, etree.QName(ATOM_NS, "title"))
        atom_title.text = title

        # atom updated
        atom_updated = etree.SubElement(entry, etree.QName(ATOM_NS, "updated"))
        atom_updated.text = datetime.now(dateutil.tz.tzutc()).isoformat()

        # atom author
        atom_author = etree.SubElement(entry, etree.QName(ATOM_NS, "author"))
        atom_author.text = "UC Libraries Digital Collection"

        # atom links - Merritt is reading the component objects from here
        link_md = etree.SubElement(entry, etree.QName(ATOM_NS, "link"), rel="alternate", href=full_metadata_url, type="application/xml", title="Full metadata for this object from Nuxeo")

        #link_media_json = etree.SubElement(root, etree.QName(ATOM_NS, "link"), rel="alternate", href=media_json_url, type="application/json", title="Deep Harvest metadata for this object") # FIXME

        link_object_file = etree.SubElement(entry, etree.QName(ATOM_NS, "link"), rel="alternate", href=nuxeo_file_download_url) # add content_type

        #########################################
        # DC namespace elements (used by Merritt)
        #########################################
        # dc creator
        for creator_name in creator_names:
            dc_creator = etree.SubElement(entry, etree.QName(DC_NS, "creator"))
            dc_creator.text = creator_name

        # dc title
        dc_title = etree.SubElement(entry, etree.QName(DC_NS, "title"))
        dc_title.text = title

        # dc date
        dc_date = etree.SubElement(entry, etree.QName(DC_NS, "date"))
        dc_date.text = date

        # dc identifier (a.k.a. local identifier) - Nuxeo ID
        nuxeo_identifier = etree.SubElement(entry, etree.QName(DC_NS, "identifier"))
        nuxeo_identifier.text = nxid
        
        # UCLDC identifier (a.k.a. local identifier) - ucldc_schema:identifier -- this will be the ARK if we have it
        if ucldc_id:
            ucldc_identifier = etree.SubElement(entry, etree.QName(NX_NS, "identifier"))
            ucldc_identifier.text = ucldc_id
        
        # UCLDC collection identifier
        ucldc_collection_id = etree.SubElement(entry, etree.QName(NX_NS, "collection"))
        ucldc_collection_id.text = ucldc_collection

    # Publish the ATOM feed
    xml_declaration = etree.ProcessingInstruction('xml', 'version="1.0" encoding="utf-8"')
    xml_declaration_string = etree.tostring(xml_declaration, encoding=unicode) 
    feed_string = etree.tostring(feed, pretty_print=True, encoding=unicode)
    with open("nx_mrt_sample.atom", "w") as f:
        f.write(xml_declaration_string)
        f.write('\n')
        f.write(feed_string)    

    # Merritt will notify the user via email of ingest status    

def get_structural_metadata(nuxeo_id):
    """ Get media.json file. See https://github.com/ucldc/ucldc-docs/wiki/media.json """
    url = "http://s3url.aws.com/{0}-media.json".format(nuxeo_id) # FIXME
    return url

def get_full_metadata(nuxeo_id):
    """ Get full metadata via Nuxeo API """
    url = "https://nuxeo-stg.cdlib.org/Nuxeo/restAPI/default/{0}/export?format=XML".format(nuxeo_id)
    return url

def get_object_download_url(nuxeo_id, nuxeo_path):
    """ Get object file download URL """
    filename = nuxeo_path.split('/')[-1]
    url = "https://nuxeo-stg.cdlib.org/Nuxeo/nxbigfile/default/{0}/file:content/{1}".format(nuxeo_id, filename)
    return url

def get_object_view_url(nuxeo_id):
    """ Get object view URL """
    url = "https://nuxeo-stg.cdlib.org/Nuxeo/nxdoc/default/{0}/view_documents".format(nuxeo_id)
    return url

if __name__ == "__main__":
    sys.exit(main())
