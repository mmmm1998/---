#!/usr/bin/env python3
# encoding: utf-8

import os
import re
from lxml.html import parse
from lxml.builder import E
from urllib.request import urlopen 

def _normalize_views_count(views_string):
    """
    Transform views string from habrahabr to number
    Example: '3k' -> 3000, '10' -> 10, '3,2k' -> 3200
        :param views_string: views string from habrahabr
        :return: views count number
        :rtype: int
    """
    r = re.search(r'([0-9]+\,[0-9]|[0-9]+)(k|m)?',views_string)
    # if number part of string is float, replace ',' to '.' (different float notation)
    num_part = float(r.group(1).replace(',','.') if ',' in r.group(1) else r.group(1))
    if r.group(2) == 'k':
        mult_part = 1000
    else:
        mult_part = 1

    try:
        return int(num_part*mult_part)
    except:
        return -1

def _body2text(body):
    """
    Transform html tree of article body to plain text (ignore code)
        :param body: html tree of article body
        :return: plaint text of article body
        :rtype: string
    """
    # TODO: improve this?
    tmp = body
    for elem in tmp.findall('.//code'):
        elem.getparent().remove(elem)
    return tmp.text_content()

_find_tags = {
    'title': '//h1[@class="post__title post__title_full"]/span[@class="post__title-text"]',
    'author': '//span[@class="user-info__nickname user-info__nickname_small"]',
    'body': '//div[@class="post__text post__text-html js-mediator-article"]',
    'rating': '//span[@class="voting-wjt__counter voting-wjt__counter_positive  js-score"]',
    'comments count': '//strong[@class="comments-section__head-counter"]',
    'views count': '//span[@class="post-stats__views-count"]',
    'bookmarks count': '//span[@class="bookmark__counter js-favs_count"]'
}
def parseHabr(link):
    """
    Parse habrahabr link and return data dictionary, with article title (title), article body (body),
    article rating (rating), article comments count (comments), article views count (views) and count of people,
    which bookmark this article (bookmarks).
        :param link: habrahabr link
        :return: data dictionary
    """
    post = {
        'title': None,
        'body': None, 
        'author': None, 
        'rating': None, 
        'comments': None,
        'views': None,
        'bookmarks': None
        }

    try:
        data = parse(urlopen(link))
    except IOError as e:
        print("parseHabr link error: "+e)
        return None

    post['title'] = data.find(_find_tags['title']).text
    try:
        post['author'] = data.find(_find_tags['author']).text
    except Exception as e:
        print("parseHabr error: "+e)
        post['author'] = None

    try:
        post['body'] = _body2text(data.find(_find_tags['body']))
    except Exception as e:
        print("parseHabr error: "+e)
        post['body'] = None

    try:
        post['rating'] = int(data.find(_find_tags['rating']).text)
    except Exception as e:
        print("parseHabr error: "+e)
        post['rating']=None

    try:
        # TODO: If comments count > 1000, found string will be '1k'? 
        # Maybe use `_normalize_views_count`?
        post['comments'] = int(data.find(_find_tags['comments count']).text)
    except Exception as e:
        print("parseHabr error: "+e) 
        post['comments'] = None

    try:
        raw_views = data.find(_find_tags['views count']).text
        post['views'] = _normalize_views_count(raw_views)
    except Exception as e:
        print("parseHabr error: "+e)
        post['views'] = None

    try:
        post['bookmarks'] = int(data.find(_find_tags['bookmarks count']).text)
    except Exception as e:
        print("parseHabr error: "+e)
        post['bookmarks'] = None

    return post
