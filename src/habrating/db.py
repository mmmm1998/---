import sqlite3
import asyncio
import re
from collections import Counter

from . import logger
from . import parser

def init_text_db(path_to_database):
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
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                author_karma INTEGER NOT NULL,
                author_rating INTEGER NOT NULL,
                author_followers INTEGER NOT NULL,
                rating INTEGER NOT NULL,
                comments INTEGER NOT NULL,
                views INTEGER NOT NULL,
                bookmarks INTEGER NOT NULL
            )
            """)
        db.commit()
    except Exception as e:
        logger.warn("database error: "+repr(e))
    finally:
        db.close()

def append_to_text_db(data, path_to_database, open_database = None):
    """
    Insert parsed post data into database
        :param data: parsed post data
        :param path_to_database: path to database
    """
    try:
        if not open_database:
            db = sqlite3.connect(path_to_database)
        else:
            logger.info('Use already open db')
            db = open_database
        cursor = db.cursor()
        cursor.execute(
            """
            INSERT INTO DATA
                (title, body, author_karma, author_rating, author_followers, rating, comments, views, bookmarks)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (data['title'], data['body'], data['author karma'], data['author rating'],
                data['author followers'], data['rating'], data['comments'], data['views'],
                data['bookmarks'])
            )
        db.commit()
    except Exception as e:
        logger.warn(f'error while insert entry into database: {repr(e)}')
    finally:
        if not open_database:
            db.close()

def save_hub_to_text_db(hub_name, path_to_database, year_filter=None):
    """
    Save all hub's posts to database
        :param hub_name: name of hub
        :param path_to_database: path to database
        :param year_filter: posts younger, that year_filter, will be ignored
    """
    init_text_db(path_to_database)
    articles = parser.get_all_hub_article_urls(hub_name)
    ioloop = asyncio.get_event_loop()
    threads_count = 12 # Habr accept 24 and less connections 
    dateArray = []
    index = 0
    while index < len(articles):
        tasks = []
        for i in range(index,min(index+threads_count, len(articles))):
            tasks.append(asyncio.ensure_future(parser.parse_article(articles[i], year_filter=year_filter)))
        index += threads_count
        dateArray += filter(lambda x: x is not None, ioloop.run_until_complete(asyncio.gather(*tasks)))
    logger.info(f"parsed {len(dateArray)} articles from hub '{hub_name}'")
    db = sqlite3.connect(path_to_database)
    try:
        for parsed_date in dateArray:
            append_to_text_db(parsed_date,path_to_database, db)
    finally:
        db.close()

def _loaded_data_to_parsed_data(loaded_data):
    data = {}
    # loaded_data[0] is index, ignore it
    data['title'] = loaded_data[1]
    data['body'] = loaded_data[2]
    data['author karma'] = loaded_data[3]
    data['author rating'] = loaded_data[4]
    data['author followers'] = loaded_data[5]
    data['rating'] = loaded_data[6]
    data['comments'] = loaded_data[7]
    data['views'] = loaded_data[8]
    data['bookmarks'] = loaded_data[9]
    return data

def load_text_db(path_to_database):
    """
    Load all parsed data from database
        :param path_to_database: path to database
    """
    try:
        db = sqlite3.connect(path_to_database)
        cursor = db.cursor()
        data = []
        for row in cursor.execute('SELECT * FROM DATA'):
            data.append(_loaded_data_to_parsed_data(row))
        return data
    except Exception as e:
        logger.warn(f'error while select data from database: {repr(e)}')
    finally:
        db.close()

def _make_words_space(data, cutoff=2, max_size=5000):
    """
    Create word space from parsed article data
        :param data: list of article texts
        :param cutoff: minimal entries count for a word to go to dict
        :param max_size: maximal dimension of word space
        :return: dict mapping word to its index in word space vector
    """
    logger.info ("Preparing to make word space")
    counter = Counter ()
    for post in data:
        words = re.split('[^a-z|а-я|A-Z|А-Я]', post['body'])
        words = map(str.lower, filter(None, words))
        counter += Counter(words)
    wordsList = {}
    idx = 0
    for word in counter.most_common(max_size):
        if word[1] <= cutoff:
            break
        wordsList[word[0]] = idx
        idx += 1
    logger.info (f'Word space dimension: {len (wordsList)}')
    return wordsList

def _vectorize_text(data, word_space):
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

def cvt_text_db_to_vec_db(path_to_database, path_to_vectorize_database):
    """
    Read all data from hub database, transform each post data text
    to vector in word spaces and save result as new database.
        :param path_to_database: database with hub data
        :param path_to_vectorize_database: path to new database with vectorize hub data
    """
    all_data = load_text_db(path_to_database)
    words_space = _make_words_space(all_data)
    for post_data in all_data:
        _vectorize_text(post_data, words_space)
    init_vec_db(path_to_vectorize_database)
    db = sqlite3.connect(path_to_vectorize_database)
    try:
        for parsed_date in all_data:
            append_to_vec_db(parsed_date,path_to_database, db)
    finally:
        db.close()

def init_vec_db(path_to_database):
    """
    Create database for vectorize data from habrahabr
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
                title TEXT NOT NULL,
                text_vector TEXT NOT NULL,
                author_karma INTEGER NOT NULL,
                author_rating INTEGER NOT NULL,
                author_followers INTEGER NOT NULL,
                rating INTEGER NOT NULL,
                comments INTEGER NOT NULL,
                views INTEGER NOT NULL,
                bookmarks INTEGER NOT NULL
            )
            """)
        db.commit()
    except Exception as e:
        logger.warn("database error: "+repr(e))
    finally:
        db.close()

def _text_vector_to_str(vector):
    return ' '.join(str(x) for x in vector)

def append_to_vec_db(data, path_to_database, open_database = None):
    """
    Insert vectorize post data into database
        :param data: parsed post data
        :param path_to_database: path to database
    """
    try:
        if not open_database:
            db = sqlite3.connect(path_to_database)
        else:
            logger.info('Using already opened db')
            db = open_database
        cursor = db.cursor()
        cursor.execute(
            """
            INSERT INTO DATA
                (title, text_vector, author_karma, author_rating, author_followers, rating, comments, views, bookmarks)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (data['title'], _text_vector_to_str(data['body']), data['author karma'], data['author rating'],
                data['author followers'], data['rating'], data['comments'], data['views'],
                data['bookmarks'])
            )
        db.commit()
    except Exception as e:
        logger.warn(f'error while insert entry into database: {repr(e)}')
    finally:
        if not open_database:
            db.close()

def _loaded_vectorize_data_to_parsed_data(loaded_data):
    data = _loaded_data_to_parsed_data(loaded_data)
    data['body'] = [int(x) for x in data['body'].split()]
    return data

def load_vec_db(path_to_database):
    """
    Load all vectorize parsed data from database
        :param path_to_database: path to database
    """
    try:
        db = sqlite3.connect(path_to_database)
        cursor = db.cursor()
        data = []
        for row in cursor.execute('SELECT * FROM DATA'):
            data.append(_loaded_vectorize_data_to_parsed_data(row))
        return data
    except Exception as e:
        logger.warn(f'error while select data from database: {repr(e)}')
    finally:
        db.close()