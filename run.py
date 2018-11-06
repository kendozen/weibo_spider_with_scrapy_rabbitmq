#!/usr/bin/env python
# encoding: utf-8
from scrapy import cmdline
import  logging

logging.basicConfig(level=logging.ERROR)
cmdline.execute("scrapy crawl SinaSpider".split(" "))
