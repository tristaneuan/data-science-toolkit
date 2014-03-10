import nltk
import os
import requests
import socket
import types
import xmltodict
from boto import connect_s3
from boto.s3.key import Key
from flask.ext import restful
from gzip import open as gzopen

NOUN_TAGS = ['NP', 'NN', 'NNS', 'NNP', 'NNPS']
S3_BUCKET = None
XML_PATH = '/data/xml/'
MEMOIZED_JSON = {}


def asList(value):
    """
    Determines if the value is a list and wraps a singleton into a list if
    necessary, done to handle the inconsistency in xml to dict
    """
    return value if isinstance(value, types.ListType) else [value]


def isEmptyDoc(doc):
    """
    Lets us know if the document is empty

    :param doc: a dict object corresponding to an xml document
    """
    return doc.get('root', {}).get('document', {}).get('sentences',
                                                       None) is None


def get_s3_bucket():
    """
    Accesses an S3 connection for us, memoized

    :return: s3 connection
    :rtype :class:boto.s3.connection.S3Connection
    """
    global S3_BUCKET
    if S3_BUCKET is None:
        S3_BUCKET = connect_s3().get_bucket('nlp-data')
    return S3_BUCKET


class PhraseService():
    """
    Not restful, allows us to abstract out what nodes we want from a tree
    parse
    """

    @staticmethod
    def get(doc_id, phrase_types):
        jsonResponse = ParsedJsonService().get(doc_id)
        if jsonResponse['status'] != 200:
            return []
        return PhraseService.phrases_from_json(jsonResponse[doc_id],
                                               asList(phrase_types))

    @staticmethod
    def phrases_from_json(json_parse, phrase_types):
        return [' '.join(f.leaves()) for sentence in
                asList(json_parse.get('root', {}).get(
                    'document', {}).get('sentences', {}).get('sentence', []))
                for f in nltk.Tree.parse(sentence.get('parse', '')).subtrees()
                if f.node in phrase_types] if not isEmptyDoc(json_parse) else []


class RestfulResource(restful.Resource):
    """
    Wraps restful.Resource to allow additional logic
    """

    def nestedGet(self, doc_id, backoff=None):
        """
        Allows us to call a service and extract data from its response

        :param doc_id: the id of the document
        :param backoff: default value
        """
        return self.get(doc_id).get(doc_id, backoff)


class ParsedXmlService(RestfulResource):
    """
    Read-only service responsible for accessing XML from FS
    """

    def get(self, doc_id):
        """
        Right now just points to new s3 method, just didn't want to remove the
        old logic just yet.

        :param doc_id: the doc id
        """
        return self.get_from_s3(doc_id)

    def get_from_s3(self, doc_id):
        """
        Returns a response with the XML of the parsed text

        :param doc_id: the id of the document in Solr
        """
        try:
            bucket = get_s3_bucket()
            key = Key(bucket)
            key.key = 'xml/%s/%s.xml' % tuple(doc_id.split('_'))

            if key.exists():
                response = {'status': 200, doc_id: key.get_contents_as_string()}
            else:
                response = {'status': 500, 'message': 'Key does not exist'}
            return response
        except socket.error:
            # probably need to refresh our connection
            global S3_BUCKET
            S3_BUCKET = None
            return self.get_from_s3(doc_id)

    def get_from_file(self, doc_id):
        """
        Return a response with the XML of the parsed text
        :param doc_id: the id of the document in Solr
        """

        response = {}
        (wid, id) = doc_id.split('_')
        xmlPath = '%s/%s/%s/%s.xml' % (XML_PATH, wid, id[0], id)
        gzXmlPath = xmlPath + '.gz'
        if os.path.exists(gzXmlPath):
            response['status'] = 200
            response[doc_id] = ''.join(gzopen(gzXmlPath).readlines())
        elif os.path.exists(xmlPath):
            response['status'] = 200
            response[doc_id] = ''.join(open(xmlPath).readlines())
        else:
            response['status'] = 500
            response['message'] = 'File not found for document %s' % doc_id
        return response


class ParsedJsonService(RestfulResource):
    """
    Read-only service responsible for accessing XML and transforming it to JSON
    Uses the ParsedXmlService
    """

    def get(self, doc_id):
        """
        Returns document parse as JSON
        :param doc_id: the id of the document in Solr
        """

        global MEMOIZED_JSON

        response = MEMOIZED_JSON.get(doc_id, {})

        if len(response) == 0:
            try:
                xmlResponse = ParsedXmlService().get(doc_id)
                if xmlResponse['status'] != 200:
                    return xmlResponse
                MEMOIZED_JSON[doc_id] = {'status': 200,
                                         doc_id: xmltodict.parse(
                                             xmlResponse[doc_id])}
                response = MEMOIZED_JSON[doc_id]
            except Exception as e:
                return {'status': 500, 'message': str(e)}
        return response


def field_for_wiki(wid, field, default=None):
    path = '/data/wiki_xml/%s/%s.xml' % (wid, field)
    if not os.path.exists(path):
        return default

    text = open(path, 'r').read()
    if len(text) > 0:
        return xmltodict.parse(text)

    return default


def phrases_for_wiki_field(wid, field):
    return PhraseService.phrases_from_json(field_for_wiki(wid, field, {}),
                                           NOUN_TAGS)


def main_page_nps(wid):
    response = requests.get('http://search-s10:8983/solr/main/select',
                            params=dict(q='wid:%s AND is_main_page:true' % wid,
                                        fl='id', wt='json'))

    docs = response.json().get('response', {}).get('docs', [{}])
    if not docs:
        return []
    doc_id = docs[0].get('id', None)

    return PhraseService.phrases_from_json(
        ParsedJsonService().nestedGet(
            doc_id, {}), NOUN_TAGS) if doc_id is not None else []
