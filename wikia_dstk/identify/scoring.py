from __future__ import division
from collections import defaultdict
from constants import *
from preprocessing import get_subdomain, preprocess, to_list


class Field(object):
    #def __init__(self, wid, name, is_url, dx_method, use_tf, weight):
    def __init__(self, wid, name, is_url, dx_method, use_tf, weight):
        self.wid = wid
        self.name = name

        self.get_list = self._get_plain_list
        if is_url:
            self.get_list = self._get_url_list

        if dx_method == SOLR:
            self._extract_data = self._extract_via_solr
        elif dx_method == SERVICE:
            self._extract_data = self._extract_via_service
        elif dx_method == SCRAPE:
            self._extract_data = self._extract_via_scrape
        else:
            raise ValueError('Invalid data extraction method')

        self.build_dict = self._build_bin_dict
        if use_tf:
            self.build_dict = self._build_tf_dict

    def get_list(self):
        pass

    def _build_dict(self, terms):
        """
        To be implemented by child classes.
        """
        pass


class BinaryField(object):
    def __init__(self, terms):
        """Class whose main purpose is to build a simple lookup dictionary from
        a list of terms in a given field, and to save it in order to facilitate
        binary scoring.
        Applicable fields: hostname, sitename, headline, title_tag"""
        self.d = {}
        if terms:
            self.d = dict((preprocess(term), 1.0) for term in terms)

    def _build_dict(self, terms):
        if terms:
            return dict((preprocess(term), 1.0) for term in terms)
        return {}

    def score(self, candidate):
        """Return a score based on the binary presence or absence of a given
        candidate in a simple lookup dictionary for a certain field. """
        return self.d.get(candidate, 0.0)


class TermFreqField(object):
    def __init__(self, terms):
        """Class whose main purpose is to build a term count dictionary from a
        list of terms in a given field, and to save the maximum term count in
        order to facilitate term frequency calculations.
        Applicable fields: domains, description, top titles, top categories"""
        self.d = defaultdict(int)
        for term in terms:
            normalized = preprocess(term)
            self.d[normalized] += 1
        self.max_score = None
        if len(self.d.values()) > 0:
            self.max_score = max(self.d.values())

    def score(self, candidate):
        """Return a score based on the term frequency of a given candidate in a
        term count dictionary for a certain field."""
        if self.max_score is not None:
            return self.d.get(candidate, 0.0) / self.max_score
        return 0.0

if __name__ == '__main__':
    foo = BinaryField(['bar'])
    assert foo.score(('bar',)) == 1.0
    assert foo.score(('baz',)) == 0.0

    foo = TermFreqField(['bar', 'bar', 'baz'])
    assert foo.score(('bar',)) == 1.0
    assert foo.score(('baz',)) == 0.5
    assert foo.score(('luhrmann',)) == 0.0
