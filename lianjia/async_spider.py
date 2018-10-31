# coding: utf-8

import multiprocessing as mp

from client.redis_client import redis_client
from lianjia.lian_jia_estate_spider import LianJiaEstateSpider
from lianjia.lianjia_city_spider import LianJiaCitySpider


def run(cls_instance, i):
    return cls_instance.batch_proccess()


if __name__ == '__main__':
    pool = mp.Pool()
    # spider_url = redis_client.lpop(LianJiaCitySpider().LIAN_JIA_CITY_URL)
    # while True and spider_url:
    #     print('--------spider url------{}'.format(spider_url))
    #     cls_instance = LianJiaEstateSpider(spider_url)
    #     pool.apply_async(run())
    #
    # spider_url = redis_client.lpop(LianJiaCitySpider().LIAN_JIA_CITY_URL)
    # print('--------update spider url------{}'.format(spider_url))

