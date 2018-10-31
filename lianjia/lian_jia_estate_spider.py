# coding: utf-8
import csv
import datetime
import json
import re

from bs4 import BeautifulSoup

from client.redis_client import redis_client
from lianjia.base_spider import BaseSpider
from lianjia.constants import CSV_HEAD_NAME_DICT, ALL_CITY_CHOICES


class LianJiaEstateSpider(BaseSpider):

    def __init__(self, base_url):
        self.base_url = base_url
        self.city = re.findall(r"[a-zA-Z_]+", re.findall(r"/[a-zA-Z_]+\.", self.base_url)[0])[0]
        self.city_redis_key = 'lianjia:{city}'.format(city=self.city)
        self.seen_redis_key = 'lianjia:seen:link:{city}'.format(city=self.city)
        self.seen = set(redis_client.lrange(self.seen_redis_key, 0, -1))
        self.unseen = set([])

    def parse(self, html):
        soup = BeautifulSoup(html, features='lxml')
        tags = soup.find_all(name='a', attrs={'target': '_blank', 'data-el': 'ershoufang'})
        return set([tag['href'] for tag in tags])

    def estate_parse(self, url):
        print('parse url:{} begin...'.format(url))
        info_json = {}

        html = self.crawl(url)
        soup = BeautifulSoup(html, features='lxml')
        info_json[CSV_HEAD_NAME_DICT.get('spider_url')] = url
        self._price_estate_parse(soup.find(name='div', attrs={'class': 'price'}), info_json)
        self._house_estate_parse(soup.find(name='div', attrs={'class': 'houseInfo'}), info_json)
        self._around_estate_parse(soup.find(name='div', attrs={'class': 'aroundInfo'}), info_json)
        self._intro_content_estate_parse(soup.find(name='div', attrs={'class': 'introContent'}), info_json)
        self._property_features(soup.find(name='div', attrs={'class': 'showbasemore'}), info_json)
        print('parse url:{} end...'.format(url))
        return info_json

    def _price_estate_parse(self, price_tags, info_json):
        info_json[CSV_HEAD_NAME_DICT.get('total')] = u'{}万'.format(
            price_tags.find('span', attrs={'class': 'total'}).string)
        info_json[CSV_HEAD_NAME_DICT.get('unitPriceValue')] = u'{}'.format(
            price_tags.find('span', attrs={'class': 'unitPriceValue'}).text)
        return info_json

    def _house_estate_parse(self, house_info_tags, info_json):
        for tag in house_info_tags:
            info_json[CSV_HEAD_NAME_DICT.get(tag.attrs['class'][0])] = u'{}|{}'.format(tag.contents[0].string,
                                                                                       tag.contents[1].string)
        return info_json

    def _around_estate_parse(self, around_info_tags, info_json):
        for tag in around_info_tags:
            if tag.attrs['class'][0] == 'communityName':
                info_json[CSV_HEAD_NAME_DICT.get('communityName')] = tag.find('a', attrs={'class': 'info'}).get_text()
            if tag.attrs['class'][0] == 'areaName':
                info_json[CSV_HEAD_NAME_DICT.get('areaName')] = u'{}/{}'.format(
                    tag.findAll('a', attrs={'target': '_blank'})[0].get_text(),
                    tag.findAll('a', attrs={'target': '_blank'})[1].get_text())
        return info_json

    def _intro_content_estate_parse(self, intro_content_tags_info_json, info_json):
        info_json[CSV_HEAD_NAME_DICT.get('base_content')] = '|'.join(
            [u'{}:{}'.format(li_tag.span.string, li_tag.get_text()) for li_tag in
             intro_content_tags_info_json.findAll('div', attrs={'class': 'content'})[
                 0].findAll('li')])

        info_json[CSV_HEAD_NAME_DICT.get('transaction_content')] = '|'.join(
            [u'{}:{}'.format(li_tag.findAll('span')[0].string, li_tag.findAll('span')[1].string) for li_tag in
             intro_content_tags_info_json.findAll('div', attrs={'class': 'content'})[
                 1].findAll('li')])

        return info_json

    def _property_features(self, property_features, info_json):
        house_tag = property_features.find('div', attrs={'class': 'tags'})
        if not house_tag:
            return info_json
        house_text = u'房源标签:{}-{}'.format(
            house_tag.find('a', attrs={'class': 'taxfree'}).get_text() if house_tag.find('a', attrs={
                'class': 'taxfree'}) else '',
            house_tag.find('a',
                           attrs={'class': 'is_see_free'}).get_text() if house_tag.find(
                'a', attrs={'class': 'is_see_free'}) else '')
        base_attribute_tags = property_features.findAll('div', attrs={'class': 'baseattribute'})
        base_attribute_text = '|'.join([u'{}:{}'.format(tag.find('div', attrs={'class': 'name'}).string,
                                                        tag.find('div', attrs={'class': 'content'}).string) for tag in
                                        base_attribute_tags])

        info_json[CSV_HEAD_NAME_DICT.get('property_features')] = u'{}|{}'.format(house_text, base_attribute_text)
        return info_json

    def output(self, url):
        info_json = self.estate_parse(url)
        id = re.findall(r"\d+", url)[0]
        redis_client.hset(self.city_redis_key, id, json.dumps(info_json))

    def batch_proccess(self):
        try:
            for i in range(1, 101):
                self.proccess('{}pg{}/'.format(self.base_url, i))
        except Exception as ex:
            print(u'record proccessed link set {}'.format(ex))
            if len(self.seen) > 0:
                redis_client.lpush(self.seen_redis_key, self.seen)

        print('parse success export csv begin')
        self.export_csv()
        print('export csv end')
        return self.city

    def proccess(self, url):
        html = self.crawl(url)
        results = self.parse(html)
        self.unseen.update(results - self.seen)
        map(lambda href: self.output(href), results)
        self.seen.update(self.unseen)
        self.unseen.clear()

    def byteify(self, input, encoding='utf-8'):
        if isinstance(input, dict):
            return {self.byteify(key): self.byteify(value) for key, value in input.iteritems()}
        elif isinstance(input, list):
            return [self.byteify(element) for element in input]
        elif isinstance(input, unicode):
            return input.encode(encoding)
        else:
            return input

    def export_csv(self):
        file_name = u'链家_{}_{}.csv'.format(
            dict(ALL_CITY_CHOICES).get(self.city), datetime.datetime.now().strftime('%Y%m%d')).encode('utf8')
        spider_data_dict = self._get_spider_data()
        with open(file_name, 'w', ) as csvfile:
            fieldnames = [value for (key, value) in CSV_HEAD_NAME_DICT.items()]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()

            if spider_data_dict:
                for (key, value) in spider_data_dict.items():
                    writer.writerow(self.byteify(json.loads(value, encoding='utf8')))

    def _get_spider_data(self):
        return redis_client.hgetall(self.city_redis_key)


if __name__ == '__main__':
    LianJiaEstateSpider(base_url='https://wh.lianjia.com/ershoufang/').batch_proccess()
