import asyncio
import re
import pickle
import progressbar
import numpy as np
from collections import Counter
from sklearn.feature_extraction.text import CountVectorizer

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

def append_db(data, path_to_file, open_stream = None):
    """
    Insert parsed post data into data file
        :param data: parsed post data
        :param path_to_file: path to data file
    """
    try:
        if not open_stream:
            with open(path_to_file,'ab') as fout:
                pickle.dump(data,fout)
        else:
            pickle.dump(data,open_stream)
    except Exception as e:
        logger.warn(f'error: {repr(e)}')

def save_db(data, path_to_file):
    try:
        with open(path_to_file,'wb') as fout:
            for post in data:
                append_db(post, None, open_stream=fout)
    except Exception as e:
        logger.warn(f'error: {repr(e)}')

def save_hub_to_db(hub_name, file_path, max_year=None, threads_count=16, operations=2,
        start_index=1):
    """
    Save all hub's posts to data file
        :param hub_name: name of hub
        :param file_path: path to loaded data file
        :param max_year: posts younger, that max_year, will be ignored
    """
    print(f'[{start_index}/{operations}]')

    urls = parser.get_all_hub_article_urls(hub_name, threads_count=threads_count)

    ioloop = asyncio.get_event_loop()
    memo = {}

    print(f'[{start_index+1}/{operations}]')
    bar = utils.get_bar(len(urls)).start()

    with open(file_path,'wb') as fout:
        for index in range(0, len(urls), threads_count):
            tasks = []
            for url in urls[index:min(index + threads_count, len(urls))]:
                # closure
                async def parse_and_save():
                    post = await parser.parse_article(url, year_up_limit=max_year, author_memoization=memo)
                    if post is not None:
                        append_db(post, None, open_stream=fout)

                tasks.append(asyncio.ensure_future(parse_and_save()))
            bar.update(index)
            ioloop.run_until_complete(asyncio.gather(*tasks))
    bar.finish()

def load_db(path_to_file):
    """
    Load all parsed data from data file
        :param path_to_file: path to data file
    """
    try:
        with open(path_to_file,'rb') as fin:
            data = []
            loaded = 0
            bar = progressbar.ProgressBar(
                widgets=
                    [
                    '[Loading db]',
                    progressbar.Counter(format='[loaded %s entries]')
                    ],
                maxval=progressbar.UnknownLength)
            bar.start()
            while True:
                try:
                    data.append(pickle.load(fin))
                    loaded += 1
                    bar.update(loaded)
                except EOFError:
                    break 
            bar.finish()
            logger.info(f'load {len(data)} posts data')
            return data
    except Exception as e:
        logger.warn(f'error: {repr(e)}')

def _fit_text_transformers(data, cutoff=2, text_max_size=5000, title_max_size=500):
    """
    Create word space from parsed article data
        :param data: list of article texts
        :param cutoff: minimal entries count for a word to go to dict
        :param max_size: maximal dimension of word space. If equals -1, dimension unlimied
        :return: dict mapping word to its index in word space vector
    """
    textes = [post['body'] for post in data]
    titles = [post['title'] for post in data]
    body_transformer = CountVectorizer(max_features=text_max_size, dtype=np.int8)
    title_transformer = CountVectorizer(max_features=title_max_size, dtype=np.int8)
    body_transformer.fit(textes)
    title_transformer.fit(titles)
    return body_transformer, title_transformer

def vectorize_post(post, body_vectorizer, title_vectorizer):
    post['body'] = body_vectorizer.transform([post['body']]).toarray()[0]
    post['title'] = title_vectorizer.transform([post['title']]).toarray()[0]

def cvt_text_db_to_vec_db(path_to_text_file, path_to_vectorize_file, path_to_words_space_file,
        operations=2, start_index=1):
    """
    Read all data from hub data file, transform each post data text
    to vector in word spaces and save result as new data file.
        :param path_to_text_file: path to text data file
        :param path_to_vectorize_file: path to new data file with vectorize hub data
	    :param disable_words_limit: if True, then disable limit on words space
    """
    all_data = load_db(path_to_text_file)
    print(f'[{start_index}/{operations}]')
    print('Long sklearn operation without any verbose output')
    body_vectorizer, title_vectorizer = _fit_text_transformers(all_data)
    print(f'[{start_index+1}/{operations}]')
    bar = utils.get_bar(len(all_data)).start()
    with open(path_to_vectorize_file,'wb') as fout:
        for index, post in enumerate(all_data):
            vectorize_post(post, body_vectorizer, title_vectorizer)
            append_db(post, path_to_vectorize_file, fout)
            bar.update(index)
    bar.finish()

    save_hub_vectorizers(path_to_words_space_file, body_vectorizer, title_vectorizer)

def save_hub_vectorizers(file_path, body_vectorizer, title_vectorizer):
    with open(file_path,'wb') as fout:
        pickle.dump(body_vectorizer,fout)
        pickle.dump(title_vectorizer,fout)

def load_words_space(words_space_file_path):
    with open(words_space_file_path,'rb') as fin:
        body_vectorizer = pickle.load(fin)
        title_vectorizer = pickle.load(fin)
    return body_vectorizer, title_vectorizer

def cvt_db_to_DataFrames(path_to_db):
    data = load_db(path_to_db)
    return cvt_to_DataFrames(data)

def cvt_to_DataFrames(data):
    feature_keys = [key for key in data[0].keys() if key not in ['rating', 'body', 'title']]
    X = [np.concatenate([d['body'], d['title'], [d[key] for key in feature_keys]]) for d in data]
    y = [d['rating'] for d in data]
    return np.asarray(X, dtype=np.float32), np.asarray(y,dtype=np.float32)
