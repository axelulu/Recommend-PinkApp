import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
PYSPARK_PYTHON = "/usr/local/python3/bin/python3"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_181-cloudera"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
# 注意，如果是使用jupyter或ipython中，利用spark streaming链接kafka的话，必须加上下面语句
# 同时注意：spark version>2.2.2的话，pyspark中的kafka对应模块已被遗弃，因此这里暂时只能用2.2.2版本的spark
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.2 pyspark-shell"
from online import stream_c, SIMILAR_DS, pool
from datetime import datetime
from setting.default import DefaultConfig
import redis
import json
import time
import setting.logging as lg
import logging
from online import HOT_DS, NEW_ARTICLE_DS

# 添加到需要打印日志内容的文件中
logger = logging.getLogger('online')


class OnlineRecall(object):
    """在线处理计算平台
    """

    def __init__(self):
        self.client = redis.StrictRedis(host=DefaultConfig.REDIS_HOST,
                                        port=DefaultConfig.REDIS_PORT,
                                        db=10)
        # 在线召回筛选TOP-k个结果
        self.k = 20

    def _update_hot_redis(self):
        """更新热门文章  click-trace
        :return:
        """
        client = self.client

        def updateHotArt(rdd):
            for row in rdd.collect():
                logger.info("{}, INFO: {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row))
                # 如果是曝光参数，和阅读时长选择过滤
                if row['param']['action'] == 'exposure' or row['param']['action'] == 'read':
                    pass
                else:
                    # 解析每条行为日志，然后进行分析保存点击，喜欢，分享次数，这里所有行为都自增1
                    client.zincrby("ch:{}:hot".format(row['category_id']), 1, row['param']['postId'])

        HOT_DS.map(lambda x: json.loads(x[1])).foreachRDD(updateHotArt)

        return None

    def _update_new_redis(self):
        """更新频道新文章 new-article
        :return:
        """
        client = self.client

        def computeFunction(rdd):
            for row in rdd.collect():
                category_id, post_id = row['category_id'], row['param']['postId']
                logger.info("{}, INFO: get kafka new_article each data:category_id{}, post_id{}".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), category_id, post_id))
                client.zadd("ch:{}:new".format(category_id), {post_id: time.time()})

        NEW_ARTICLE_DS.map(lambda x: json.loads(x[1])).foreachRDD(computeFunction)

        return None

    def _update_content_recall(self):
        """
        在线内容召回计算
        :return:
        """

        # {"actionTime":"2019-04-10 21:04:39","readTime":"","category_id":18,
        # "param":{"action": "click", "userId": "2", "post_id": "116644", "algorithmCombine": "C2"}}
        # x [,'json.....']
        def get_similar_online_recall(rdd):
            """
            解析rdd中的内容，然后进行获取计算
            :param rdd:
            :return:
            """
            # rdd---> 数据本身
            # [row(1,2,3), row(4,5,6)]----->[[1,2,3], [4,5,6]]
            import happybase
            # 初始化happybase连接
            pool = happybase.ConnectionPool(size=3, host='hadoop102',
                                            autoconnect=True, transport='framed', protocol='compact')
            for data in rdd.collect():

                # 进行data字典处理过滤
                if data['param']['action'] in ["click", "collect", "share"]:

                    logger.info(
                        "{} INFO: get user_id:{} action:{}  log".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                        data['param']['userId'],
                                                                        data['param']['action']))

                    # 读取param当中post_id，相似的文章
                    with pool.connection() as conn:

                        sim_table = conn.table("article_similar")

                        # 根据用户点击流日志涉及文章找出与之最相似文章(基于内容的相似)，选取TOP-k相似的作为召回推荐结果
                        _dic = sim_table.row(str(data["param"]["postId"]).encode(), columns=[b"similar"])
                        _srt = sorted(_dic.items(), key=lambda obj: obj[1], reverse=True)  # 按相似度排序
                        if _srt:

                            topKSimIds = [int(i[0].split(b":")[1]) for i in _srt[:10]]

                            # 根据历史推荐集过滤，已经给用户推荐过的文章
                            history_table = conn.table("history_recall")

                            _history_data = history_table.cells(
                                b"reco:his:%s" % data["param"]["userId"].encode(),
                                b"category:%d" % data["category_id"]
                            )

                            history = []
                            if len(_history_data) > 1:
                                for l in _history_data:
                                    history.extend(l)

                            # 根据历史召回记录，过滤召回结果
                            recall_list = list(set(topKSimIds) - set(history))

                            # 如果有推荐结果集，那么将数据添加到cb_recall表中，同时记录到历史记录表中
                            logger.info(
                                "{} INFO: store online recall data:{}".format(
                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(recall_list)))

                            if recall_list:
                                recall_table = conn.table("cb_recall")

                                recall_table.put(
                                    b"recall:user:%s" % data["param"]["userId"].encode(),
                                    {b"online:%d" % data["category_id"]: str(recall_list).encode()}
                                )

                                history_table.put(
                                    b"reco:his:%s" % data["param"]["userId"].encode(),
                                    {b"category:%d" % data["category_id"]: str(recall_list).encode()}
                                )
                        conn.close()

        # x可以是多次点击行为数据，同时拿到多条数据
        SIMILAR_DS.map(lambda x: json.loads(x[1])).foreachRDD(get_similar_online_recall)

    def _update_new_redis(self):
        """更新频道新文章 new-article
        :return:
        """
        client = self.client

        def computeFunction(rdd):
            for row in rdd.collect():
                category_id, post_id = row['category_id'], row['param']['postId']
                logger.info("{}, INFO: get kafka new_article each data:category_id{}, post_id{}".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), category_id, post_id))
                client.zadd("ch:{}:new".format(category_id), {post_id: time.time()})

        NEW_ARTICLE_DS.map(lambda x: json.loads(x[1])).foreachRDD(computeFunction)

        return None


if __name__ == '__main__':
    # 启动日志配置
    lg.create_logger()
    ore = OnlineRecall()
    ore._update_content_recall()
    ore._update_hot_redis()
    ore._update_new_redis()
    print("开始监听")
    stream_c.start()
    # 使用 ctrl+c 可以退出服务
    _ONE_DAY_IN_SECONDS = 60 * 60 * 24
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        pass
