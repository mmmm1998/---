import re
import asyncio
import aiohttp
import datetime
from lxml.html import document_fromstring

from . import logger
from . import utils

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
    except ValueError:
        return None

def _normalize_rating(rating_string):
    """
    Transform rating representation to number
        :param rating_string: string with rating
    """
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
    for elem in body.findall('.//code'):
        elem.getparent().remove(elem)
    return body.text_content().lower()

async def _safe_request(link, session):
    """
    Make request and if it fails, make it again
        :param link: url for requesting
        :param session: auihttp session
    """
    max_attempt = 10
    for attempt in range(max_attempt):
        page = await session.get(link, timeout=120)
        if page.status == 200:
            return page
        if page.status >= 500:
            logger.info(f"status for {link} is {page.status}, wait 1 second")
        else:
            logger.warning(f"link {link} is not valid, return None")
            return None
        page = None
        await asyncio.sleep(1)
    return page

_find_tags = {
    'post date':'.//span[@class="post__time"]',
    'title': './/h1[@class="post__title post__title_full"]/span[@class="post__title-text"]',
    'author': './/span[@class="user-info__nickname user-info__nickname_small"]',
    'author_readonly': './/sup[@class="author-info__status"]',
    'author_karma_rating_followers': './/div[contains(@class, "stacked-counter__value")]',
    'body': './/div[@class="post__text post__text-html js-mediator-article"]',
    'rating': './/span[contains(@class, "voting-wjt__counter")]',
    'comments count': './/strong[@class="comments-section__head-counter"]',
    'views count': './/span[@class="post-stats__views-count"]',
    'bookmarks count': './/span[@class="bookmark__counter js-favs_count"]',
    'company info': './/dt[@class="profile-section__title"]',
    'company rating': './/sup[@class="page-header__stats-value page-header__stats-value_branding"]',
    'last hub page': './/a[@class="toggle-menu__item-link toggle-menu__item-link_pagination toggle-menu__item-link_bordered"]',
    'hub pages': './/a[@class="toggle-menu__item-link toggle-menu__item-link_pagination"]'
}

async def parse_article(link, year_up_limit = None, author_memoization=None):
    """
    Parse article and return dictionary with info about it:
    its title, body, body length, author karma, author rating, author followers count, author company rating,
    rating, comments count, views count and bookmarked count.

    Therefore, the returned dict will have the following keys:
    title, body, body length, author karma, author rating, author followers, company
    rating, rating, comments, views, bookmarks.

    Async function.    
        :param link: url to article
        :param year_up_limit: posts younger, that year_up_limit, will be ignored
        :param author_memoization: distionary to memoization author features
        :return: dict described above
    """
    post = {
        'title': None,
        'year': None,
        'body': None, 
        'body length': None,
        'author karma': None, 
        'author rating': None,
        'author followers': None,
        'company rating': None,
        'rating': None, 
        'comments': None,
        'views': None,
        'bookmarks': None
        }

    async with aiohttp.ClientSession() as session:
        try:
            page = await _safe_request(link, session)
            pageHtml = await page.text()
            data = document_fromstring(pageHtml)
        except IOError as e:
            logger.warning(f'link error: {repr(e)}')
            return None

        try:
            datastr = data.find(_find_tags['post date']).text.lstrip(' ')
            is_writed_yesterday = 'вчера' in datastr
            is_writed_today = 'сегодня' in datastr
            is_writed_in_this_year = datastr.split(' ')[2] == 'в'
            if is_writed_in_this_year or is_writed_today or is_writed_yesterday:
                year = datetime.datetime.now().year
            else:
                year = int(datastr.split(' ')[2])
            post['year'] = year
        except Exception as e:
            logger.info(f"page {link}")
            logger.warning(f"error while parse year: {repr(e)}")
            post['year'] = None

        if (year_up_limit is not None) and (year > year_up_limit):
            logger.info(f"post {link} writed in {year} but limit is {year_up_limit}, so drop")
            return None

        try:
            post['title'] = data.find(_find_tags['title']).text
        except Exception as e:
            logger.info(f"page {link}")
            logger.warning(f"error while parse title: {repr(e)}")
            post['title'] = None

        try:
            element = data.find(_find_tags['company rating'])
            if element is not None:
                post['company rating'] = float(element.text.replace(',','.').replace(' ',''))
            else:
                post['company rating'] = 0.0
        except Exception as e:
            logger.info(f"page {link}")
            logger.warning(f"error while parse company rating: {repr(e)}")
            post['company rating'] = None

        try:
            author = data.find(_find_tags['author']).text
            if author_memoization is not None and author in author_memoization:
                post['author karma'] = author_memoization[author][0]
                post['author rating'] = author_memoization[author][1]
                post['author followers'] = author_memoization[author][2]
            else:
                author_page = await _safe_request(f'https://habrahabr.ru/users/{author}', session)
                author_page_html = await author_page.text()
                author_page_tree = document_fromstring(author_page_html)
                author_showing = author_page_tree.xpath(_find_tags['author_karma_rating_followers'])
                if len(author_showing) == 3:
                    post['author karma'] = _normalize_views_count(author_showing[0].text)
                    post['author rating'] = _normalize_views_count(author_showing[1].text)
                    post['author followers'] = _normalize_views_count(author_showing[2].text)
                else:
                    tmp = author_page_tree.find(_find_tags['author_readonly'])
                    if len(author_showing) == 0 and tmp is not None and tmp.text == 'read-only':
                        logger.info(f'Author for {link} is "{author}", who read-only')
                        post['author karma'] = 0
                        post['author rating'] = 0
                        post['author followers'] = 0
                    else:
                        raise Exception("problem with tags of data showings")
                if author_memoization is not None:
                    author_memoization[author] = (post['author karma'], post['author rating'], post['author followers'])
        except Exception as e:
            logger.info(f"page {link}")
            logger.warning(f"error while parse author '{author}': {repr(e)}")
            post['author karma'] = None
            post['author rating'] = None
            post['author followers'] = None

    try:
        post['body'] = _body2text(data.find(_find_tags['body']))
        post['body length'] = len(post['body'])
    except Exception as e:
        logger.info(f"page {link}")
        logger.warning(f"error while parse post body: {repr(e)}")
        post['body'] = None
        post['body length'] = None

    try:
        raw_rating = data.xpath(_find_tags['rating'])[0].text
        post['rating'] = _normalize_rating(raw_rating)
    except Exception as e:
        logger.info(f"page {link}")
        logger.warning(f"error while parse rating: {repr(e)}")
        post['rating']=None

    try:
        post['comments'] = int(data.find(_find_tags['comments count']).text)
    except Exception as e:
        logger.info(f"page {link}")
        logger.warning(f"error while parse comments: {repr(e)}")
        post['comments'] = None

    try:
        raw_views = data.find(_find_tags['views count']).text
        post['views'] = _normalize_views_count(raw_views)
    except Exception as e:
        logger.info(f"page {link}")
        logger.warning(f"error while parse views: {repr(e)}")
        post['views'] = None

    try:
        post['bookmarks'] = int(data.find(_find_tags['bookmarks count']).text)
    except Exception as e:
        logger.info(f"page {link}")
        logger.warning(f"error while parse bookmarks: {repr(e)}")
        post['bookmarks'] = None

    return post

def _is_page_nonempty(page_html):
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

async def _get_hub_last_page(hub):
    """
    Get last page of target hub
        :param hub: hub name
    """
    async with aiohttp.ClientSession() as session:
        try:
            url = 'https://habrahabr.ru/hub/'+hub+'/all/page1'
            page_response = await _safe_request(url, session)
            page_html = await page_response.text()
        except Exception as e:
            logger.error(f"link {url}: "+repr(e))
            return None
        data = document_fromstring(page_html)
        last_page_element = data.find(_find_tags["last hub page"])
        if last_page_element is  None:
            # Hub too small, that link to last page in direct page link, so get last
            last_page_element = data.xpath(_find_tags['hub pages'])[-1]
        last_page = int(last_page_element.attrib["href"].lstrip(f'/hub/{hub}/all/page').rstrip('/'))
        return last_page

async def get_articles_from_page(page_url):
    """
    For the specified hub page return all articles contained in this page.
    Async function.
        :param page_url: url of the page
        :return: list of hrefs to articles
    """
    async with aiohttp.ClientSession() as session:
        try:
            page_response = await _safe_request(page_url, session)
            page_html = await page_response.text()
        except Exception as e:
            logger.warning(f"link {page_url}: "+repr(e))
            return None

        if _is_page_nonempty(page_html):
            data = document_fromstring(page_html)
            page_articles = _pagebody2articles(data)
            return page_articles
        else:
            return []

def get_all_hub_article_urls(hub, threads_count=20):
    """
    For the specified hub return all articles belonging to this hub.
        :param hub: name of the hub
        :return: list of hrefs to articles
    """
    baseurl = 'https://habrahabr.ru/hub/'+hub+'/all/'
    articles = []

    ioloop = asyncio.get_event_loop()
    last_page_number = ioloop.run_until_complete(_get_hub_last_page(hub))
    logger.info(f'found {last_page_number} pages in {hub} hub')

    bar = utils.get_bar(last_page_number).start()
    for page_number in range(1, last_page_number+1, threads_count):
        bar.update(page_number-1)
        tasks = []
        for i in range(page_number, min(page_number+threads_count, last_page_number+1)):
            page_url = baseurl+'page'+str(i)
            logger.info(f"load hub '{hub}' page {str(i)}")
            tasks.append(asyncio.ensure_future(get_articles_from_page(page_url)))
        results = ioloop.run_until_complete(asyncio.gather(*tasks))
        for result in results:
            articles += result
    bar.finish()

    return articles
