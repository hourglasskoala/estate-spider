# coding: utf-8
import json
import re

from bs4 import BeautifulSoup

from client.redis_client import redis_client
from lianjia.base_spider import BaseSpider


class LianJiaCitySpider(BaseSpider):
    LIAN_JIA_CITY_URL = 'lian_jia_city_url'

    def parse(self, html):
        soup = BeautifulSoup(html, features='lxml')
        tags = soup.find_all(name='a', attrs={'href': re.compile('^https://\w*.?\w*.lianjia.com/$')})
        return set(['{}ershoufang/'.format(url['href']) for url in tags])

    def output(self, results):
        map(lambda result: redis_client.lpush(self.LIAN_JIA_CITY_URL, result), results)

    def proccess(self, url):
        html = self.crawl(url)
        results = self.parse(html)
        self.output(results)


if __name__ == '__main__':
    base_url = 'https://bj.lianjia.com/city'
    LianJiaCitySpider().proccess(base_url)
