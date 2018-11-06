# encoding: utf-8
import random
from source.sina.cookies import cookies, init_cookies
from source.sina.user_agents import agents
from scrapy.http import Request
from source.sina.spiders.weibo_spider import *


class UserAgentMiddleware(object):
    """ 换User-Agent """

    def process_request(self, request, spider):
        agent = random.choice(agents)
        request.headers["User-Agent"] = agent


class CookiesMiddleware(object):
    """ 换Cookie """

    def __init__(self, connection_url, queue_name, cookie_count):
        cookie_count = cookie_count if cookie_count > 0 else 5
        init_cookies(connection_url, queue_name, cookie_count)

    def process_request(self, request, spider):
        if len(cookies) == 0:
            time.sleep(1)

        cookie = random.choice(cookies)
        request.cookies = cookie

    # 判断cookie是否失效，如果失效，则将cookies移除
    # def process_response(self, request, response, spider):
    #     pass

    def from_settings(settings):
        connection_url = settings.get('RABBITMQ_CONNECTION_PARAMETERS')
        queue_name = settings.get('RABBITMQ_COOKIE_QUEUE_NAME')
        cookie_count = settings.get('COOKIE_COUNT')
        return CookiesMiddleware(connection_url, queue_name, cookie_count)


class CookieInvalidCheckerMiddleware(object):
    """检测Cookie是否失效，如果失效，则重新请求"""

    def process_response(self, request, response, spider):
        if response.status == 302:
            current_cookie = str(request.cookies)
            for cookie in cookies:
                if str(cookie) == current_cookie:
                    cookies.remove(cookie)
                    break
            return request
        else:
            return response
        # if '退出' not in response.text:
        #     spider.crawler.engine.slot.scheduler.enqueue_request(request)
        #     return Request(request.url, callback=request.callback, dont_filter=True)
        # return response
