from __future__ import division
from collections import defaultdict
from preprocessing import get_subdomain, preprocess, to_list


class Field(object):
    def __init__(self, data, is_url, use_tf, weight):
        # Get lists of NPs or raw data, depending on the field
        if is_url:
            self.data = [get_subdomain(url) for url in to_list(data)]
        else:
            self.data = to_list(data)

        if use_tf:
            self._dict = self._build_tf_dict()
            self.score = self._score_tf
        else:
            self._dict = self._build_bin_dict()
            self.score = self._score_bin

        self.weight = weight
        self._max_score = None

    def _build_tf_dict(self):
        if self.data:
            d = defaultdict(int)
            for term in self.data:
                normalized = preprocess(term)
                d[normalized] += 1
            if len(d.values()) > 0:
                self._max_score = max(d.values())
            return d
        return {}

    def _build_bin_dict(self):
        if self.data:
            return dict((preprocess(term), 1.0) for term in self.data)
        return {}

    def _score_tf(self, candidate):
        return self._dict.get(candidate, 0.0) * self.weight

    def _score_bin(self, candidate):
        if self._max_score is not None:
            return ((self._dict.get(candidate, 0.0) / self._max_score) *
                    self.weight)
        return 0.0


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
