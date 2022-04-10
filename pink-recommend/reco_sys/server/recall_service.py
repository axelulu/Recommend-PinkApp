import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))

from server import redis_client
from server import pool
import logging
from datetime import datetime
from server.utils import HBaseUtils

logger = logging.getLogger('recommend')


class ReadRecall(object):
    """读取召回集的结果
    """

    def __init__(self):
        self.client = redis_client
        self.hbu = HBaseUtils(pool)
        self.hot_num = 10

    def read_hbase_recall_data(self, table_name, key_format, column_format):
        """
        读取cb_recall当中的推荐数据
        读取的时候可以选择列族进行读取als, online, content

        :return:
        """
        recall_list = []
        try:
            data = self.hbu.get_table_cells(table_name, key_format, column_format)

            # data是多个版本的推荐结果[[],[],[],]
            for _ in data:
                recall_list = list(set(recall_list).union(set(eval(_))))

            # self.hbu.get_table_delete(table_name, key_format, column_format)
        except Exception as e:
            logger.warning("{} WARN read {} recall exception:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                        table_name, e))
        return recall_list

    def read_redis_new_article(self, category_id):
        """
        读取新闻章召回结果
        :param category_id: 提供频道
        :return:
        """
        logger.warning("{} WARN read category {} redis new article".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                          category_id))
        _key = "ch:{}:new".format(category_id)
        try:
            res = self.client.zrevrange(_key, 0, -1)
        except Exception as e:
            logger.warning(
                "{} WARN read new article exception:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))
            res = []

        return list(map(int, res))

    def read_redis_hot_article(self, category_id):
        """
        读取新闻章召回结果
        :param category_id: 提供频道
        :return:
        """
        logger.warning("{} WARN read category {} redis hot article".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                          category_id))
        _key = "ch:{}:hot".format(category_id)
        try:
            res = self.client.zrevrange(_key, 0, -1)

        except Exception as e:
            logger.warning(
                "{} WARN read new article exception:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))
            res = []

        # 由于每个频道的热门文章有很多，因为保留文章点击次数
        res = list(map(int, res))
        if len(res) > self.hot_num:
            res = res[:self.hot_num]
        return res

    def read_hbase_article_similar(self, table_name, key_format, article_num):
        """获取文章相似结果
        :param post_id: 文章id
        :param article_num: 文章数量
        :return:
        """
        # 第一种表结构方式测试：
        # create 'article_similar', 'similar'
        # put 'article_similar', '1', 'similar:1', 0.2
        # put 'article_similar', '1', 'similar:2', 0.34
        try:
            _dic = self.hbu.get_table_row(table_name, key_format)

            res = []
            _srt = sorted(_dic.items(), key=lambda obj: obj[1], reverse=True)
            if len(_srt) > article_num:
                _srt = _srt[:article_num]
            for _ in _srt:
                res.append(int(_[0].decode().split(':')[1]))
        except Exception as e:
            logger.error(
                "{} ERROR read similar article exception: {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))
            res = []
        return res


if __name__ == '__main__':
    rr = ReadRecall()
    # 召回结果的读取封装
    print(rr.read_hbase_recall_data('cb_recall', b'recall:user:2079499053372018688', b'als:2'))
