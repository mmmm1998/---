#!/usr/bin/env python3
# encoding: utf-8

import unittest
import sys
import colour_runner.runner as crr
sys.path.append('../src')

import habrParser as hp

class TestViewsNormalize(unittest.TestCase):
    def test1(self):
        s = '3,2k'
        e = 3200
        self.assertEqual(hp._normalize_views_count(s),e)
    def test2(self):
        s = '15'
        e = 15
        self.assertEqual(hp._normalize_views_count(s),e)
    def test3(self):
        s = '14,4k'
        e = 14400
        self.assertEqual(hp._normalize_views_count(s),e)
    def test4(self):
        s = '20k'
        e = 20000
        self.assertEqual(hp._normalize_views_count(s),e)

class Test_vectorize_data_post_text(unittest.TestCase):
    def test1(self):
        space = ['except', 'one', 'of', 'this', 'movie', 'man', 'have',
            'cat', 'five', 'magic', 'far', 'logic', 'perfect']

        s = {'body': "one of this man have five cat"}
        e = {'body': [0, 1, 1, 1, 0, 1, 1, 1, 1, 0, 0, 0, 0]}

        hp._vectorize_data_post_text(s, space)
        self.assertEqual(s,e)

if __name__ == '__main__':
    unittest.main(testRunner=crr.ColourTextTestRunner, verbosity=2) 