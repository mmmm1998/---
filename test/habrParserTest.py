#!/usr/bin/env python3
# encoding: utf-8

import unittest
import sys
import colour_runner.runner as crr
sys.path.append('../src')

from habrating import parser, db

class TestViewsNormalize(unittest.TestCase):
    def test1(self):
        s = '3,2k'
        e = 3200
        self.assertEqual(parser._normalize_views_count(s),e)
    def test2(self):
        s = '15'
        e = 15
        self.assertEqual(parser._normalize_views_count(s),e)
    def test3(self):
        s = '14,4k'
        e = 14400
        self.assertEqual(parser._normalize_views_count(s),e)
    def test4(self):
        s = '20k'
        e = 20000
        self.assertEqual(parser._normalize_views_count(s),e)

class TestVectorizeText(unittest.TestCase):
    def test1(self):
        space = {'except': 0, 'one': 1, 'of': 2, 'this': 3, 'movie': 4, 'man': 5, 'have': 6,
            'cat': 7, 'five': 8, 'magic': 9, 'far': 10, 'logic': 11, 'perfect':12}

        s = {'body': "one of this man have five cat"}
        e = {'body': [0, 1, 1, 1, 0, 1, 1, 1, 1, 0, 0, 0, 0]}

        db.vectorize_text(s, space)
        self.assertEqual(s,e)

if __name__ == '__main__':
    unittest.main(testRunner=crr.ColourTextTestRunner, verbosity=2) 