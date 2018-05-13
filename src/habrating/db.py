import sqlite3
import asyncio
import re
import pickle
import pandas as pd
from collections import Counter

from . import logger
from . import parser
from . import utils

def init_db(path_to_file):
    """
    Create data file for parsed data from habrahabr
        :param path_to_file: path where to create pickle object
        :return: None
    """
    try:
        file = open(path_to_file,'wb')
        file.close()
    except Exception as e:
        logger.warn("error: "+repr(e))

def append_to_db(data, path_to_file, open_stream = None):
    """
    Insert parsed post data into data file
        :param data: parsed post data
        :param path_to_file: path to data file
    """
    try:
        if not open_stream:
            with open(path_to_file,'wb') as fout:
                pickle.dump(data,fout)
        else:
            pickle.dump(data,open_stream)
    except Exception as e:
        logger.warn(f'error: {repr(e)}')

def save_hub_to_db(hub_name, file_path, max_year=None):
    """
    Save all hub's posts to data file
        :param hub_name: name of hub
        :param file_path: path to loaded data file
        :param max_year: posts younger, that max_year, will be ignored
    """
    print('[1/3]')

    threads_count = 12 # Habr accepts n <= 24 connections
    init_db(file_path)
    urls = parser.get_all_hub_article_urls(hub_name)

    ioloop = asyncio.get_event_loop()
    articles = []
    memo = {}

    print('[2/3]')
    bar = utils.get_bar(len(urls)).start()
    for index in range(0, len(urls), threads_count):
        tasks = []
        for url in urls[index:min(index + threads_count, len(urls))]:
            tasks.append(asyncio.ensure_future(
                parser.parse_article(url, year_up_limit=max_year, author_memoization=memo)))
        bar.update(index)
        articles += filter(lambda x: x is not None, ioloop.run_until_complete(asyncio.gather(*tasks)))
    bar.finish()
    logger.info(f"parsed {len(articles)} articles from hub '{hub_name}'")

    print('[3/3]')
    bar = utils.get_bar(len(articles)).start()
    fout = open(file_path,'wb')
    try:
        for index, parsed_date in enumerate(articles):
            append_to_db(parsed_date, file_path, fout)
            bar.update(index)
    finally:
        fout.close()
        bar.finish()

def load_db(path_to_file):
    """
    Load all parsed data from data file
        :param path_to_file: path to data file
    """
    try:
        with open(path_to_file,'rb') as fin:
            data = []
            while True:
                try:
                    data.append(pickle.load(fin))
                except EOFError:
                    break 
            logger.info(f'load {len(data)} posts data')
            return data
    except Exception as e:
        logger.warn(f'error: {repr(e)}')

def _make_words_space(data, cutoff=2, max_size=5000):
    """
    Create word space from parsed article data
        :param data: list of article texts
        :param cutoff: minimal entries count for a word to go to dict
        :param max_size: maximal dimension of word space. If equals -1, dimension unlimied
        :return: dict mapping word to its index in word space vector
    """
    logger.info ("Preparing to make word space")
    counter = Counter ()
    bar = utils.get_bar(len(data)).start()
    for index, post in enumerate(data):
        words = re.split('[^a-z|а-я|A-Z|А-Я]', post['body'])
        words = map(str.lower, filter(None, words))
        counter += Counter(words)
        bar.update(index)
    bar.finish()
    wordsList = {}
    idx = 0
    words = counter.most_common(max_size) if max_size != -1 else counter.most_common()
    for word in words:
        if word[1] <= cutoff:
            break
        wordsList[word[0]] = idx
        idx += 1
    logger.info (f'Word space dimension: {len (wordsList)}')
    return wordsList

def vectorize_text(data, word_space):
    """
    Replace data post text by vector of words space.
    Vector consists of zeros and ones, where one mean, that
    words in words space with this index contains in post text
        :param data: parsed post data
        :param words_space: result of _make_words_space function
    """
    vector = [0] * len(word_space)
    words = re.split('[^a-z|а-я|A-Z|А-Я]', data['body'])
    for word in map(str.lower,words):
        idx = word_space.get(word)
        if idx != None:
            vector[idx] = 1
    data['body'] = vector

def cvt_text_db_to_vec_db(path_to_text_file, path_to_vectorize_file, path_to_words_space_file, disable_words_limit=False):
    """
    Read all data from hub data file, transform each post data text
    to vector in word spaces and save result as new data file.
        :param path_to_text_file: path to text data file
        :param path_to_vectorize_file: path to new data file with vectorize hub data
	    :param disable_words_limit: if True, then disable limit on words space
    """
    all_data = load_db(path_to_text_file)
    print('[1/3]')
    words_space = _make_words_space(all_data, max_size =-1) if disable_words_limit else _make_words_space(all_data)
    print('[2/3]')
    bar = utils.get_bar(len(all_data)).start()
    for index, post_data in enumerate(all_data):
        vectorize_text(post_data, words_space)
        bar.update(index)
    bar.finish()

    print('[3/3]')
    bar.start()

    init_db(path_to_vectorize_file)
    fout = open(path_to_vectorize_file,'wb')
    try:
        for index, parsed_date in enumerate(all_data):
            append_to_db(parsed_date, path_to_vectorize_file, fout)
            bar.update(index)
    finally:
        fout.close()
        bar.finish()

    pickle.dump(words_space,open(path_to_words_space_file,'wb'))

def load_words_space(words_space_file_path):
    return pickle.load(open(words_space_file_path,'rb'))

def cvt_db_to_DataFrames(path_to_db):
    data = load_db(path_to_db)
    return cvt_to_DataFrames(data)

def cvt_to_DataFrames(data):
    X = []
    y = []
    for d in data:
        y.append(d['rating'])

        row = []
        for key in d.keys():
            if key not in ['rating', 'body', 'title']:
                row.append(d[key])
        # TODO: vectorize title too and add to features
        row += d['body']

        X.append(row)
    return pd.DataFrame(X), pd.DataFrame(y)
