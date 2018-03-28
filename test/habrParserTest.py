#!/usr/bin/env python3
# encoding: utf-8

import unittest
import sys
sys.path.append('../src')

import habrParser as hp

class TestViewsNormalize(unittest.TestCase):
    def test1(self):
        s = '3,2k'
        e = 3200
        self.assertEqual(hp._normalize_views_count(s),e)

if __name__ == '__main__':
    unittest.main() 
