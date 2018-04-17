#!/usr/bin/env python3
# encoding: utf-8

import os
import re
import asyncio
import aiohttp
import sqlite3
from lxml.html import parse, document_fromstring
from lxml.builder import E
from urllib.request import urlopen 
from collections import Counter


def _normalize_views_count(views_string):
    """
    Transform views count (given as a string) from
    habrahabr's format to an integer
    Example: '3k' -> 3000, '10' -> 10, '3,2k' -> 3200
        :param views_string: view count in habr's format
        :return: views count
        :rtype: int
    """
    r = re.search(r'([0-9]+\,[0-9]|[0-9]+)(k|m)?',views_string)
    # if number part of string is float, replace ',' to '.' (different float notation)
    num_part = float(r.group(1).replace(',','.') if ',' in r.group(1) else r.group(1))
    if r.group(2) == 'k':
        mult_part = 1000
    elif r.group(2) == 'm':
        mult_part = 1000000
    else:
        mult_part = 1

    try:
        return int(num_part*mult_part)
    except:
        return -1

def _normalize_rating(rating_string):
    if '–' in rating_string:
        rating_string = rating_string.replace('–','-')
    return int(rating_string)

def _body2text(body):
    """
    Transform html tree of article body to plain normalize text (ignoring code)
        :param body: html tree of article body
        :return: plain text of article
        :rtype: string
    """
    # TODO: omit images too
    for elem in body.findall('.//code'):
        elem.getparent().remove(elem)
    return body.text_content().lower()

_find_tags = {
    'title': './/h1[@class="post__title post__title_full"]/span[@class="post__title-text"]',
    'author': './/span[@class="user-info__nickname user-info__nickname_small"]',
    'body': './/div[@class="post__text post__text-html js-mediator-article"]',
    'rating': './/span[contains(@class, "voting-wjt__counter")]',
    'comments count': './/strong[@class="comments-section__head-counter"]',
    'views count': './/span[@class="post-stats__views-count"]',
    'bookmarks count': './/span[@class="bookmark__counter js-favs_count"]'
}
# Sven: maybe rename this to `parseArticle'?
async def parseHabr(link):
    """
    Parse article and return dictionary with info about it:
    its title, body, rating, comments count, views count and bookmarked count.
    Therefore, the returned dict will have the following keys:
    title, body, rating, comments, views, bookmarks.
    Async function.
        :param link: url to article
        :return: dict described above
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
        async with aiohttp.ClientSession() as session:
            while True:
                page = await session.get(link, timeout=120)
                if page.status == 200:
                    break
                print(f"status for {link} is {page.status}, wait 1 second")
                await asyncio.sleep(1)
            pageHtml = await page.text()
            data = document_fromstring(pageHtml)
    except IOError as e:
        print("parseHabr link error: "+e.args[0])
        return None

    post['title'] = data.find(_find_tags['title']).text
    try:
        post['author'] = data.find(_find_tags['author']).text
    except Exception as e:
        print("parseHabr error: "+e.args[0])
        print(f"link: {link}")
        post['author'] = None

    try:
        post['body'] = _body2text(data.find(_find_tags['body']))
    except Exception as e:
        print("parseHabr error: "+e.args[0])
        print(f"link: {link}")
        post['body'] = None

    try:
        raw_rating = data.xpath(_find_tags['rating'])[0].text
        post['rating'] = _normalize_rating(raw_rating)
    except Exception as e:
        print("parseHabr error: "+e.args[0])
        print(f"link: {link}")
        post['rating']=None

    try:
        # TODO: If comments count > 1000, found string will be '1k'? 
        # Maybe use `_normalize_views_count`?
        post['comments'] = int(data.find(_find_tags['comments count']).text)
    except Exception as e:
        print("parseHabr error: "+e.args[0])
        print(f"link: {link}") 
        post['comments'] = None

    try:
        raw_views = data.find(_find_tags['views count']).text
        post['views'] = _normalize_views_count(raw_views)
    except Exception as e:
        print("parseHabr error: "+e.args[0])
        print(f"link: {link}")
        post['views'] = None

    try:
        post['bookmarks'] = int(data.find(_find_tags['bookmarks count']).text)
    except Exception as e:
        print("parseHabr error: "+e.args[0])
        print(f"link: {link}")
        post['bookmarks'] = None

    return post

def _is_page_contains_article(page_html):
    """
    Check if hub page contains articles.
        :param: html text of page
        :return: True or False
    """
    found = page_html.find('empty-placeholder__message')
    return found == -1

def _pagebody2articles(tree):
    """
    Parse DOM tree of hub page and return list of all
    hrefs to articles on this page.
        :param tree: DOM tree of page
        :return: all links to articles from this page
    """
    hrefs = tree.findall('.//a[@class="post__title_link"]')
    return list(map(lambda href: href.attrib['href'], hrefs))

async def get_articles_from_page(page_url):
    """
    For the specified hub page return all articles contained in this page.
    Async function.
        :param page_url: url of the page
        :return: list of hrefs to articles
    """
    async with aiohttp.ClientSession() as session:
        try:
            while True:
                page_response = await session.get(page_url, timeout=120)
                if page_response.status == 200:
                    break
                print(f"code {page_response.status} for {page_url}, wait 1 second")
                await asyncio.sleep(1)
        except Exception as e:
            print("parseHabr link error: "+e.args[0])
            print(f"link: {page_url}")
            return None
        page_html = await page_response.text()
        if _is_page_contains_article(page_html):
            data = document_fromstring(page_html)
            page_articles = _pagebody2articles(data)    
            return page_url, page_articles
        else:
            return page_url, []

def get_all_hub_article_urls(hub):
    """
    For the specified hub return all articles belonging to this hub.
        :param hub: name of the hub
        :return: list of hrefs to articles
    """
    threads_count = 24 # Habr accept 24 and less connections?
    baseurl = 'https://habrahabr.ru/hub/'+hub+'/all/'
    page_number = 1
    articles = []
    is_pages_end = False
    ioloop = asyncio.get_event_loop()
    while not is_pages_end:
        tasks = []
        for i in range(page_number,page_number+threads_count):
            page_url = baseurl+'page'+str(i)
            tasks.append(asyncio.ensure_future(get_articles_from_page(page_url)))
        page_number += threads_count
        results = ioloop.run_until_complete(asyncio.gather(*tasks))
        for url, result in results:
            if len(result) == 0 and not is_pages_end:
                is_pages_end = True
            articles += result
    return articles

def init_parsed_habr_data_db(path_to_database):
    """
    Create database for parsed data from habrahabr
        :param path_to_database: path where to create database
        :return: None
    """
    try:
        db = sqlite3.connect(path_to_database)
        cursor = db.cursor()
        cursor.execute(
            """
            CREATE TABLE DATA (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                body TEXT NOT NULL,
                author TEXT NOT NULL,
                rating INTEGER NOT NULL,
                comments INTEGER NOT NULL,
                views INTEGER NOT NULL,
                bookmarks INTEGER NOT NULL
            )
            """)
        db.commit()
    except Exception as e:
        print("habrParser db error: "+e.args[0])
    finally:
        db.close()

def _make_words_space(data):
    """
    Create words space from array of parsed article data
        :param data: list of article texts
        :return: list of all words found in articles
    """
    wordsList = {}
    for post in data:
        words = re.split('[^a-z|а-я|A-Z|А-Я]', post['body'])
        words = map(str.lower,words)
        words = list(filter(lambda x: x != '',words))
        counter = Counter(words)
        for word in counter:
            if word in wordsList:
                wordsList[word] += 1
            else:
                wordsList[word] = 1
    # Remove words found only in one post
    wordsList = dict(filter(lambda x: x[1] > 1, wordsList.items()))
    return wordsList

def _vectorize_data_post_text(data, words_space):
    """
    Replace data post text by vector of words space.
    Vector consists of zeros and ones, where one mean, that
    words in words space with this index contains in post text
        :param data: parsed post data
        :param words_space: result of _make_words_space function
    """
    vector = [0] * len(words_space)
    for word in data['body'].split():
        vector[words_space.index(word)] = 1
    data['body'] = vector

def append_parsed_habr_data_to_db(data, path_to_database):
    """
    Insert parsed post data into database
        :param data: parsed post data
        :param path_to_database: path to database
    """
    try:
        db = sqlite3.connect(path_to_database)
        cursor = db.cursor()
        cursor.execute(
            """
            INSERT INTO DATA
                (body, author, rating, comments, views, bookmarks)
            VALUES
                (?, ?, ?, ?, ?, ?);
            """,
            (data['body'], data['author'], data['rating'], data['comments'], 
                data['views'], data['bookmarks'])
            )
        db.commit()
    except Exception as e:
        print("habrParser db error: "+e.args[0])
    finally:
        db.close()

def save_hub_to_db(hub_name, path_to_database):
    """
    Save all hub's posts to database
        :param hub_name: name of hub
        :param path_to_database: path to database
    """
    init_parsed_habr_data_db(path_to_database)
    articles = get_all_hub_article_urls(hub_name)
    ioloop = asyncio.get_event_loop()
    threads_count = 24 # Habr accept 24 and less connections?
    dateArray = []
    index = 0
    while index < len(articles):
        tasks = []
        for i in range(index,min(index+threads_count, len(articles))):
            tasks.append(asyncio.ensure_future(parseHabr(articles[i])))
        index += threads_count
        dateArray += ioloop.run_until_complete(asyncio.gather(*tasks))
    for parsed_date in dateArray:
        append_parsed_habr_data_to_db(parsed_date,path_to_database)
