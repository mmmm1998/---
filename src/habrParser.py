#!/usr/bin/env python3
# encoding: utf-8

import os
import re
import sqlite3 as lite
from lxml.html import parse
from lxml.builder import E
from lxml import etree
from urllib.request import urlretrieve
from shutil import rmtree, copy2
from subprocess import call
from string import punctuation
from urllib.request import urlopen


# 3k -> 3000
# 10 -> 10
# 3,2k -> 3200
def _normalize_views_count(views_string):
    r = re.search(r'([0-9]+\,[0-9])(k|m)',views_string)
    num_part = float(r.group(1).replace(',','.'))
    if r.group(2) == 'k':
        mult_part = 1000
    else:
        mult_part = 1

    try:
        return int(num_part*mult_part)
    except:
        return -1

def _body2text(body):
    # TODO: Сделать преобразование тела поста в plain text
    return 'Not implemented, TODO body2text'

def parseHabr(link):
    try:
        post = {
            'title': None,
            'body': None, 
            'author': None, 
            'rating': None, 
            'comments': None,
            'views': None,
            'bookmarks': None
            }

        data = parse(urlopen(link))

        post['title'] = data.find('//h1[@class="post__title post__title_full"]/span[@class="post__title-text"]').text
        try:
            post['author'] = data.find('//span[@class="user-info__nickname user-info__nickname_small"]').text
        except: # TODO: test this error
            post['author'] = None

        try:
            post['body'] = _body2text(data.find('//div[@class="post__text post__text-html js-mediator-article"]'))
        except:
            post['body'] = None

        try:
            post['rating'] = data.find('//div[@class="voting   "]/div/span[@class="score"]').text
        except: 
            post['rating']=None

        try:
            # TODO: А если комментариев > 1000, то будет число, или же 1k?
            post['comments'] = int(data.find('//strong[@class="comments-section__head-counter"]').text)
        except: 
            post['comments'] = None

        try:
            views_str = data.find('//span[@class="post-stats__views-count"]').text
            post['views'] = _normalize_views_count(views_str)
        except:
            post['views'] = None

        # TODO: сделать 'bookmarks'

        return post
    except IOError as e:
        print(e)
