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

if __name__ == '__main__':
    unittest.main(testRunner=crr.ColourTextTestRunner, verbosity=2) 