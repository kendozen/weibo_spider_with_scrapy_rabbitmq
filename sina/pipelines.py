# -*- coding: utf-8 -*-
from source.sina.items import *
from source.sina.db_helper import *
import logging

logger = logging.getLogger(__name__)


class SqlServerPipeline(object):
    def __init__(self):
        pass

    def process_item(self, item, spider):
        """ 判断item的类型，并作相应的处理，再入数据库 """
        if isinstance(item, TweetsItem):
            upsert_post(dict(item))
        elif isinstance(item, InformationItem):
            upsert_info(dict(item))
        return item


def upsert_post(item):
    try:
        sql = "EXEC [dbo].[p_weibo_upsert_post] @uid = N'%s', @title = N'%s', @url = N'%s', @comments = %d, @retweets = %d, @likes = %d, @posted_time = N'%s', @post_id = N'%s'"
        sql = sql % (get_value_or_default(item, 'ID'),
                     get_value_or_default(item, 'Content'),
                     get_value_or_default(item, 'Url'),
                     get_value_or_default(item, 'Comment', 0),
                     get_value_or_default(item, 'Transfer', 0),
                     get_value_or_default(item, 'Like', 0),
                     get_value_or_default(item, 'PubTime'),
                     get_value_or_default(item, '_id'))
        execute_no_query(sql)
    except:
        logger.exception('更新文章出错.item=%s' % dict(item))


def upsert_info(item):
    try:
        sql = u"EXEC [dbo].[p_weibo_upsert_info] @uid = N'%s',@uname = N'%s',@follows = %d,@posts = %d,@labels = N'%s',@introduce = N'%s'," \
              u"@fans = %d,@verify = N'%s',@level = '%s',@verify_info = N'%s',@avatar = N'%s',@province= N'%s', @city= N'%s'"
        sql = sql % (get_value_or_default(item, '_id'),
                     get_value_or_default(item, 'NickName'),
                     get_value_or_default(item, 'Num_Follows', 0),
                     get_value_or_default(item, 'Num_Tweets', 0),
                     get_value_or_default(item, 'Labels'),
                     get_value_or_default(item, 'BriefIntroduction'),
                     get_value_or_default(item, 'Num_Fans', 0),
                     get_value_or_default(item, 'Verify'),
                     get_value_or_default(item, 'VIPlevel'),
                     get_value_or_default(item, 'Authentication'),
                     get_value_or_default(item, 'Avatar'),
                     get_value_or_default(item, 'Province'),
                     get_value_or_default(item, 'City'))
        execute_no_query(sql)
    except:
        logger.exception('更新微博账号数据出错.item=%s' % dict(item))

def get_value_or_default(item, key_name, default = ''):
    if key_name in item.keys():
        value = item[key_name]
        if isinstance(value,str):
            return value.replace("'","''")
        else:
            return value
    else:
        return default