import pymssql
import datetime
import logging

logger = logging.getLogger(__name__)

HOST = '192.168.1.60'
USERNAME = 'sa'
PASSWORD = 'topsocial2018,'
DATABASE_NAME = 'top_social_test'


def get_max_time(uid):
    #因文章每翻页都需要获取一次，另外已经有数据插入到数据库中，所以需要判断更新时间是当前时间3小时外的数据
    sql = "SELECT MAX([posted_time]) FROM [top_social_test].[dbo].[resource_posts_weibo] WHERE uid = '%s' AND datediff(HOUR,updated_time,GETDATE()) > 3" % uid
    try:
        rows = get_rows(sql)
        if len(rows) == 0 or rows[0][0] is None:
            return datetime.datetime.min
        return rows[0][0]
    except:
        logger.exception('获取文章最新更新时间出错.')
        return datetime.datetime.min


def execute_no_query(sql):
    conn = pymssql.connect(host=HOST, user=USERNAME, password=PASSWORD, database=DATABASE_NAME)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    conn.close()


def get_rows(sql):
    conn = pymssql.connect(host=HOST, user=USERNAME, password=PASSWORD, database=DATABASE_NAME)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    conn.commit()
    conn.close()
    return rows


def execute_batch(sql):
    conn = pymssql.connect(host=HOST, user=USERNAME, password=PASSWORD, database=DATABASE_NAME)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    conn.close()


def exists_with_key(table_name, key_name, key_value):
    """
    :param table_name: 待查询的表名
    :param key_name: 待查询的字段名
    :param key_value: 待查询的字段值
    :return: 是否存在
    """
    sql = "select count(*) from " + table_name + " where " + key_name + " = '%s' " % key_value
    conn = pymssql.connect(host=HOST, user=USERNAME, password=PASSWORD, database=DATABASE_NAME)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    conn.commit()
    conn.close()
    return rows[0][0] > 0
