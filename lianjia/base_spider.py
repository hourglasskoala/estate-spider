# coding: utf-8
from urllib import urlopen


class BaseSpider(object):
    def crawl(self, url):
        return urlopen(url=url).read().decode('utf-8')

    def parse(self, html):
        raise NotImplementedError()

    def output(self,results):
        raise NotImplementedError()
