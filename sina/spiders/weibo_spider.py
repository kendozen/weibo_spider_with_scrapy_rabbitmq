#!/usr/bin/env python
# encoding: utf-8

import time
import requests
import re
from lxml import etree
from scrapy import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from source.sina.items import TweetsItem, InformationItem, RelationshipsItem
from source.sina.db_helper import *


class Spider(Spider):
    name = "SinaSpider"
    host = "https://weibo.cn"
    route_key = 'weibo_resource'
    exchange_name = 'exchange_task_test'
    queue_name = 'weibo_resource_test'
    task_id = 0
    uid = ''  # 这里是多线程的，设置这个不能作为上下文来使用

    def _make_request(self, mframe, hframe, body):
        # 如果mframe = None，表示是本地读取的URL，直接访问
        if isinstance(body, str) and 'page' in body:
            return Request(body, callback=self.parse_tweets, dont_filter=True)
        else:
            if isinstance(body, str):
                return Request(body, callback=self.parse_information)
            else:
                msg = str(body, encoding="utf-8")
                data = eval(msg)
                self.uid = data.get("uid")
                self.task_id = data.get("id")
                url = 'https://weibo.cn/%s/info' % self.uid
                return Request(url, callback=self.parse_information)

    def parse_information(self, response):
        """ 抓取个人信息 """
        informationItem = InformationItem()
        selector = Selector(response)
        id = re.findall('(\d+)/info', response.url)[0]
        try:
            text1 = ";".join(selector.xpath('body/div[@class="c"]//text()').extract())  # 获取标签里的所有text()
            nickname = re.findall('昵称;?[：:]?(.*?);', text1)
            gender = re.findall('性别;?[：:]?(.*?);', text1)
            place = re.findall('地区;?[：:]?(.*?);', text1)
            briefIntroduction = re.findall('简介;?[：:]?(.*?);', text1)
            birthday = re.findall('生日;?[：:]?(.*?);', text1)
            sexOrientation = re.findall('性取向;?[：:]?(.*?);', text1)
            sentiment = re.findall('感情状况;?[：:]?(.*?);', text1)
            vipLevel = re.findall('会员等级;?[：:]?(.*?);', text1)
            authentication = re.findall('认证;?[：:]?(.*?);', text1)
            url = re.findall('互联网;?[：:]?(.*?);', text1)
            avatar = selector.xpath('body/div[@class="c"]//@src').extract_first()

            informationItem["_id"] = id
            if nickname and nickname[0]:
                informationItem["NickName"] = nickname[0].replace(u"\xa0", "")
            if gender and gender[0]:
                informationItem["Gender"] = gender[0].replace(u"\xa0", "")
            if place and place[0]:
                place = place[0].replace(u"\xa0", "").split(" ")
                informationItem["Province"] = place[0]
                if len(place) > 1:
                    informationItem["City"] = place[1]
            if briefIntroduction and briefIntroduction[0]:
                informationItem["BriefIntroduction"] = briefIntroduction[0].replace(u"\xa0", "")
            if birthday and birthday[0]:
                try:
                    birthday = datetime.datetime.strptime(birthday[0], "%Y-%m-%d")
                    informationItem["Birthday"] = birthday - datetime.timedelta(hours=8)
                except Exception:
                    informationItem['Birthday'] = birthday[0]  # 有可能是星座，而非时间
            if sexOrientation and sexOrientation[0]:
                if sexOrientation[0].replace(u"\xa0", "") == gender[0]:
                    informationItem["SexOrientation"] = "同性恋"
                else:
                    informationItem["SexOrientation"] = "异性恋"
            if sentiment and sentiment[0]:
                informationItem["Sentiment"] = sentiment[0].replace(u"\xa0", "")
            if vipLevel and vipLevel[0]:
                informationItem["VIPlevel"] = vipLevel[0].replace(u"\xa0", "")
            if authentication and authentication[0]:
                informationItem["Authentication"] = authentication[0].replace(u"\xa0", "")
            if url:
                informationItem["URL"] = url[0]
            if avatar:
                informationItem["Avatar"] = avatar

            try:
                urlothers = "https://weibo.cn/attgroup/opening?uid=%s" % id
                # new_ck = {}
                # for ck in response.request.cookies:
                #     new_ck[ck['name']] = ck['value']
                r = requests.get(urlothers, cookies=response.request.cookies, timeout=5, verify=False)
                if r.status_code == 200:
                    selector = etree.HTML(r.content)
                    texts = ";".join(selector.xpath('//body//div[@class="tip2"]/a//text()'))
                    if texts:
                        num_tweets = re.findall('微博\[(\d+)\]', texts)
                        num_follows = re.findall('关注\[(\d+)\]', texts)
                        num_fans = re.findall('粉丝\[(\d+)\]', texts)
                        if num_tweets:
                            informationItem["Num_Tweets"] = int(num_tweets[0])
                        if num_follows:
                            informationItem["Num_Follows"] = int(num_follows[0])
                        if num_fans:
                            informationItem["Num_Fans"] = int(num_fans[0])

                url_tags = 'https://weibo.cn/account/privacy/tags/?uid=%s' % id
                resp = requests.get(url_tags, cookies=response.request.cookies, timeout=5, verify=False)
                tags = ''
                if resp.status_code == 200:
                    selector = Selector(resp)
                    tag_selector = re.findall('标签:;?[::]?(.*?);设置',
                                              ';'.join(selector.xpath('body/div[@class="c"]//text()').extract()))
                    if tag_selector:
                        tags = tag_selector[0].replace(u"\xa0", "").replace(u";;", ",").replace(u";", ",").strip(',')
                informationItem['Labels'] = tags
                informationItem['Verify'] = ''
            except Exception as e:
                yield Request(response.request.url, callback=self.parse_information)
        except Exception as e:
            yield Request(response.request.url, callback=self.parse_information)
        else:
            yield informationItem

        if informationItem["Num_Tweets"] and informationItem["Num_Tweets"] > 0:
            yield Request(url="https://weibo.cn/%s?filter=1&page=1" % id, callback=self.parse_tweets,
                          dont_filter=True)

    def parse_tweets(self, response):
        """ 抓取微博数据 """

        selector = Selector(response)
        ID = re.findall('(\d+)\\?filter', response.url)[0]
        divs = selector.xpath('body/div[@class="c" and @id]')

        latest_post_time = get_max_time(ID)
        is_need_request_next = True

        for div in divs:
            try:
                tweetsItems = TweetsItem()
                id = div.xpath('@id').extract_first()  # 微博ID
                id = id.strip('M_')
                content = div.xpath('div/span[@class="ctt"]//text()').extract()  # 微博内容
                top_label = div.xpath('div/span[@class="kt"]').extract()  # 置顶标签
                cooridinates = div.xpath('div/a/@href').extract()  # 定位坐标
                like = re.findall('赞\[(\d+)\]', div.extract())  # 点赞数
                transfer = re.findall('转发\[(\d+)\]', div.extract())  # 转载数
                comment = re.findall('评论\[(\d+)\]', div.extract())  # 评论数
                others = div.xpath('div/span[@class="ct"]/text()').extract()  # 求时间和使用工具（手机或平台）
                tweetsItems['Url'] = 'https://weibo.com/%s/%s?type=comment' % (ID, id)

                tweetsItems["_id"] = ID + '_' + id
                tweetsItems["ID"] = ID
                if content:
                    tweetsItems["Content"] = " ".join(content).strip('[位置]')  # 去掉最后的"[位置]"
                if cooridinates:
                    cooridinates = re.findall('center=([\d.,]+)', cooridinates[0])
                    if cooridinates:
                        tweetsItems["Co_oridinates"] = cooridinates[0]
                if like:
                    tweetsItems["Like"] = int(like[0])
                if transfer:
                    tweetsItems["Transfer"] = int(transfer[0])
                if comment:
                    tweetsItems["Comment"] = int(comment[0])
                if others:
                    others = others[0].split('来自')
                    if len(others) == 2:
                        tweetsItems["Tools"] = others[1].replace(u"\xa0", "")
                    # 判断是否需要下一页,如果当前文章是置顶，则不做该判断
                    tweetsItems["PubTime"] = parse_time(others[0].replace(u"\xa0", ""))
                    if top_label is not None and len(top_label) > 0:  # 有置顶的页签
                        is_need_request_next = True
                    else:#TODO 这里需要优化，看是否需要放到这个循环体内来做这个判断
                        is_need_request_next = (not (latest_post_time - tweetsItems["PubTime"]).days > 0  # 比数据库中的数据要新的
                                                and (latest_post_time - tweetsItems["PubTime"]).days < 3  # 近3天采集的要更新数据
                                                and (datetime.datetime.now() - tweetsItems["PubTime"]).days <= 90)  # 3个月内的
                yield tweetsItems
            except Exception as e:
                self.logger.info(response.url)
                pass

        url_next = selector.xpath('body/div[@class="pa" and @id="pagelist"]/form/div/a[text()="下页"]/@href').extract()
        if url_next and is_need_request_next:
            yield Request(url=self.host + url_next[0], callback=self.parse_tweets, dont_filter=True)


def parse_time(posted_time):
    """
    时间格式转换
    :param time_str: 需转换的时间
    :return: 转换后的时间对象 %Y-%m-%d %H:%M:%S
    """
    posted_time = posted_time.replace("生日", "")
    if u"今天" in str(posted_time):
        time_n = re.findall(u"今天 (\d+:\d+)", str(posted_time))[0]
        posted_time = time.strftime("%Y-%m-%d", time.localtime(time.time())) + " " + time_n
    elif u"日" in str(posted_time):
        time_n = re.findall(u"日 (\d+:\d+)", str(posted_time))[0]
        time_y = re.findall(u"(\d{2})月", str(posted_time))[0]
        time_r = re.findall(u"(\d{2})日", str(posted_time))[0]
        posted_time = "2018-" + time_y + "-" + time_r + " " + time_n
    elif u"分钟前" in str(posted_time):
        time_m = re.findall(u"(\d+)分钟前", str(posted_time))[0]
        time_s = int(time_m) * 60
        posted_time = int(time.time()) - time_s
        timeArray = time.localtime(posted_time)
        posted_time = time.strftime("%Y-%m-%d %H:%M", timeArray)
    return datetime.datetime.strptime(posted_time, "%Y-%m-%d %H:%M")
