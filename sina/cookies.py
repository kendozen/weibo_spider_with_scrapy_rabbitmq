#!/usr/bin/env python
# encoding: utf-8
import time
import pika
import threading

"""
输入你的微博账号和密码，可去淘宝买，一元七个。
建议买几十个，微博反扒的厉害，太频繁了会出现302转移。
或者你也可以把时间间隔调大点。
"""


def cookie_to_dict(cookie_str):
    cookie_dict = {}
    arr = cookie_str.split(';')
    for ck in arr:
        keypair = ck.split('=')
        if len(keypair) == 2:
            cookie_dict[keypair[0]] = keypair[1]
    return cookie_dict


cookies = []


def init_cookies(connection_url, queue_name, cookie_count):
    connection = pika.BlockingConnection(pika.URLParameters(connection_url))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    for i in range(cookie_count):
        is_success, msg = get_cookie_from_mq(channel, queue_name)
        if is_success is False:
            break
        cookies.append(cookie_to_dict(msg))
    thread = threading.Thread(target=monitor_cookies, args=(connection_url, queue_name, cookie_count))
    thread.start()


def get_cookie_from_mq(channel, queue_name):
    try:
        mframe, hframe, body = channel.basic_get(queue=queue_name, no_ack=True)
        if mframe is None or mframe.message_count == 0:
            return False, None
        msg = str(body, encoding="utf-8")
        return True, msg
    except:
        raise


def monitor_cookies(connection_url, queue_name, cookie_count):
    connection = pika.BlockingConnection(pika.URLParameters(connection_url))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    while True:
        try:
            time.sleep(3)
            if len(cookies) >= cookie_count: continue

            is_success, msg = get_cookie_from_mq(channel, queue_name)
            if is_success is True:
                cookies.append(cookie_to_dict(msg))
        except:
            pass
