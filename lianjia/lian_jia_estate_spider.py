# coding: utf-8
import re

from bs4 import BeautifulSoup

from client.redis_client import redis_client
from lianjia.base_spider import BaseSpider


class LianJiaEstateSpider(BaseSpider):

    def __init__(self):
        self.base_url = 'https://sz.lianjia.com/ershoufang/'
        self.seen = set([])
        self.unseen = set([])

    def parse(self, html):
        soup = BeautifulSoup(html, features='lxml')
        tags = soup.find_all(name='a', attrs={'target': '_blank', 'data-el': 'ershoufang'})
        return set([tag['href'] for tag in tags])

    def estate_parse(self, url):
        info_json = {}

        html = self.crawl(url)
        soup = BeautifulSoup(html, features='lxml')
        self._price_estate_parse(soup.find(name='div', attrs={'class': 'price'}), info_json)
        self._house_estate_parse(soup.find(name='div', attrs={'class': 'houseInfo'}), info_json)
        self._around_estate_parse(soup.find(name='div', attrs={'class': 'aroundInfo'}), info_json)
        self._intro_content_estate_parse(soup.find(name='div', attrs={'class': 'introContent'}), info_json)
        self._property_features(soup.find(name='div', attrs={'class': 'showbasemore'}), info_json)
        return info_json

    def _price_estate_parse(self, price_tags, info_json):
        info_json['total'] = u'{}万'.format(price_tags.find('span', attrs={'class': 'total'}).string)
        info_json['unitPriceValue'] = u'{}'.format(
            price_tags.find('span', attrs={'class': 'unitPriceValue'}).text)
        return info_json

    def _house_estate_parse(self, house_info_tags, info_json):
        for tag in house_info_tags:
            info_json[tag.attrs['class'][0]] = u'{}|{}'.format(tag.contents[0].string, tag.contents[1].string)
        return info_json

    def _around_estate_parse(self, around_info_tags, info_json):
        for tag in around_info_tags:
            if tag.attrs['class'][0] == 'communityName':
                info_json['communityName'] = tag.find('a', attrs={'class': 'info'}).get_text()
            if tag.attrs['class'][0] == 'areaName':
                info_json['areaName'] = u'{}/{}'.format(tag.findAll('a', attrs={'target': '_blank'})[0].get_text(),
                                                        tag.findAll('a', attrs={'target': '_blank'})[1].get_text())
        return info_json

    def _intro_content_estate_parse(self, intro_content_tags_info_json, info_json):
        info_json['base_content'] = '|'.join([u'{}:{}'.format(li_tag.span.string, li_tag.get_text()) for li_tag in
                                              intro_content_tags_info_json.findAll('div', attrs={'class': 'content'})[
                                                  0].findAll('li')])

        info_json['transaction_content'] = '|'.join(
            [u'{}:{}'.format(li_tag.findAll('span')[0].string, li_tag.findAll('span')[1].string) for li_tag in
             intro_content_tags_info_json.findAll('div', attrs={'class': 'content'})[
                 1].findAll('li')])

        return info_json

    def _property_features(self, property_features, info_json):
        house_tag = property_features.find('div', attrs={'class': 'tags'})
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

        info_json['property_features'] = u'{}|{}'.format(house_text, base_attribute_text)
        return info_json

    def output(self, url):
        info_json = self.estate_parse(url)
        id = re.findall(r"\d+", url)[0]
        city = re.findall(r"[a-zA-Z_]+", re.findall(r"/[a-zA-Z_]+\.", url)[0])[0]
        redis_client.hset('lianjia:{city}'.format(city=city), id, info_json)

    def proccess(self, url):
        html = self.crawl(url)
        results = self.parse(html)
        self.unseen.update(results - self.seen)
        map(lambda href: self.output(href), results)
        self.seen.update(self.unseen)
        self.unseen.clear()


if __name__ == '__main__':
    base_url = 'https://sz.lianjia.com/ershoufang/pg1/'
    LianJiaEstateSpider().proccess(base_url)
