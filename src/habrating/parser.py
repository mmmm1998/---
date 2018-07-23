import scrapy
import datetime
import re
import contextlib
import os
import pickle
from scrapy.crawler import CrawlerProcess, Settings
from billiard import Process
from lxml.html import document_fromstring
from urllib.request import urlopen
from scrapy import signals
from tempfile import NamedTemporaryFile

from . import utils

class CrawlerThread(Process):
    def __init__(self, spider, settings, *args):
        Process.__init__(self)
        self.spider = spider
        self.settings = settings
        self.args = args

    def run(self):
        process = CrawlerProcess(self.settings)
        process.crawl(self.spider, *self.args)
        process.start()

class HabrHubSpider(scrapy.Spider):
    def __init__(self, hub_name, bar):
        self.name = hub_name
        self.start_urls = [f'https://habrahabr.ru/hub/{hub_name}/all/page1']
        self.bar = bar

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(HabrHubSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.finish_bar, signals.spider_closed)
        crawler.signals.connect(spider.increment_bar, signals.item_scraped)
        return spider

    def finish_bar(self):
        self.bar.finish()

    def increment_bar(self):
        self.bar.update(self.bar.currval+1)

    def __normalize_rating(self, rating_string):
        """
        Transform rating representation to number
            :param rating_string: string with rating
        """
        if '–' in rating_string:
            rating_string = rating_string.replace('–','-')
        return int(rating_string)

    def __normalize_company_rating(self, company_rating_string):
        if company_rating_string is None:
            return 0.0
        else:
            return float(company_rating_string.replace(',','.').replace(' ',''))

    def __get_year_from_datastr(self, date_string):
        is_writed_yesterday = 'вчера' in date_string
        is_writed_today = 'сегодня' in date_string
        is_writed_in_this_year = date_string.split(' ')[2] == 'в'
        if is_writed_in_this_year or is_writed_today or is_writed_yesterday:
            year = datetime.datetime.now().year
        else:
            year = int(date_string.split(' ')[2])
        return year

    def _normalize_views_count(self, views_string):
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
        except ValueError:
            return None

    def _body2text(self, body):
        """
        Transform html tree of article body to plain normalize text (ignoring code)
            :param body: html tree of article body
            :return: plain text of article
            :rtype: string
        """
        for elem in body.findall('.//code'):
            elem.getparent().remove(elem)
        return body.text_content().lower()

    def parse(self, response):
        for habr_post in response.css('a[class="post__title_link"]::attr(href)').extract():
            yield response.follow(habr_post, self.parse_article, dont_filter=True)

        next_page = response.css('a[id="next_page"]::attr(href)').extract_first()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse, dont_filter=True)

    def parse_article(self, response):
        post = {}

        # year
        # TODO year filter
        raw_data = response.css('span[class="post__time"]::text').extract_first().lstrip()
        post['year'] = self.__get_year_from_datastr(raw_data)

        # title
        post['title'] = response.css(\
            'h1[class="post__title post__title_full"] span[class="post__title-text"]::text').extract_first()

        # body and body length
        post_text_root = response.css('div[class="post__text post__text-html js-mediator-article"]')[0].root
        post['body'] = self._body2text(post_text_root)
        post['body length'] = len(post['body']) 

        # company rating
        raw_company_rating = response.css('sup[class="page-header__stats-value page-header__stats-value_branding"]::text').extract_first()
        post['company rating'] = self.__normalize_company_rating(raw_company_rating)

        # rating
        raw_rating = response.css('span[class*="voting-wjt__counter"]::text').extract_first()
        post['rating'] = self.__normalize_rating(raw_rating)

        # comments
        post['comments'] = int(response.css('strong[class="comments-section__head-counter"]::text').extract_first())

        # views
        raw_views = response.css('span[class="post-stats__views-count"]::text').extract_first()
        post['views'] = self._normalize_views_count(raw_views)

        # bookmarks
        post['bookmarks'] = int(response.css('span[class="bookmark__counter js-favs_count"]::text').extract_first())

        # author karma, rating, follower
        author = response.css('span[class="user-info__nickname user-info__nickname_small"]::text').extract_first()
        request = scrapy.Request(f'https://habrahabr.ru/users/{author}', callback=self.parse_author, dont_filter=True)
        request.meta['post'] = post
        yield request

    def parse_author(self, response):
        post = response.meta['post']
        author_parameters = response.css('div[class *="stacked-counter__value"]::text').extract()
        if len(author_parameters) == 3:
            post['author karma'] = self._normalize_views_count(author_parameters[0])
            post['author rating'] = self._normalize_views_count(author_parameters[1])
            post['author followers'] = self._normalize_views_count(author_parameters[2])
        else:
            author_status = response.css('sup[class="author-info__status"]::text').extract_first()
            if author_status == 'read-only':
                post['author karma'] = 0
                post['author rating'] = 0
                post['author followers'] = 0
            else:
                raise RuntimeError(f'problem with tags of data showings: {author_parameters}, {author_status}')

        yield post

class HabrArticleSpider(scrapy.Spider):
    def __init__(self, article_page):
        self.start_urls = [article_page]

    def parse(self, response):
        tmp = HabrHubSpider("",None)
        yield from tmp.parse_article(response)

def _get_hub_last_page(hub):
    """
    Get last page of target hub
        :param hub: hub name
    """
    url = 'https://habrahabr.ru/hub/'+hub+'/all/page1'
    data = document_fromstring(urlopen(url).read())
    last_page_xpath = './/a[@class="toggle-menu__item-link toggle-menu__item-link_pagination toggle-menu__item-link_bordered"]'
    last_page_element = data.find(last_page_xpath)
    if last_page_element is  None:
        # Hub too small, that link to last page in direct page link, so get last
        hub_pages_xpath = './/a[@class="toggle-menu__item-link toggle-menu__item-link_pagination"]'
        last_page_element = data.xpath(hub_pages_xpath)[-1]
    last_page = int(last_page_element.attrib["href"].lstrip(f'/hub/{hub}/all/page').rstrip('/'))
    return last_page

def _hub_articles_count(hub):
    last_page = _get_hub_last_page(hub)
    url = 'https://habrahabr.ru/hub/'+hub+'/all/page'+str(last_page)
    data = document_fromstring(urlopen(url).read())
    return (last_page-1)*10 + len(data.findall('.//a[@class="post__title_link"]'))

def save_hub_to_db(hub_name, file_path, max_year=None, operations=1, start_index=1):
    with contextlib.suppress(FileNotFoundError):
        os.remove(file_path)

    bar = utils.get_bar(_hub_articles_count(hub_name)).start()

    new_thread = CrawlerThread(HabrHubSpider, Settings({
        'FEED_FORMAT': 'pickle',
        'FEED_URI': f'./{file_path}',
        'LOG_LEVEL': 'ERROR',
        'RETRY_TIMES': 10
    }), hub_name, bar,)
    new_thread.start()
    new_thread.join()

def parse_article(url):
    tmp_file = NamedTemporaryFile()
    new_thread = CrawlerThread(HabrArticleSpider, Settings({
        'FEED_FORMAT': 'pickle',
        'FEED_URI': f'{tmp_file.name}',
        'LOG_LEVEL': 'ERROR',
        'RETRY_TIMES': 10,
        'ITEM_PIPELINES': {'habrating.parser.SingletonrStorePipeline': 300}
    }), url)
    new_thread.start()
    new_thread.join()
    return pickle.load(tmp_file)
    
