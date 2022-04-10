import json
import os
import sys

# 4.---------user_article_basic,hbase:user_profile
# 如果当前代码文件运行测试需要加入修改路径，避免出现后导包问题
BASE_DIR = os.path.dirname(os.path.dirname(os.getcwd()))
sys.path.insert(0, os.path.join(BASE_DIR))
PYSPARK_PYTHON = "/usr/local/python3/bin/python3"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_181-cloudera"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
from offline import SparkSessionBase

import pyhdfs
import time


class UpdateUserProfile(SparkSessionBase):
    """离线相关处理程序
    """
    SPARK_APP_NAME = "updateUser"
    SPARK_URL = "yarn"

    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()


uup = UpdateUserProfile()
# 手动关联所有日期文件
import pandas as pd
from datetime import datetime


def datelist(beginDate, endDate):
    date_list = [datetime.strftime(x, '%Y-%m-%d') for x in list(pd.date_range(start=beginDate, end=endDate))]
    return date_list


dl = datelist("2021-12-06", time.strftime("%Y-%m-%d", time.localtime()))

fs = pyhdfs.HdfsClient(hosts='hadoop102:9870')
uup.spark.sql("use profile")
for d in dl:
    try:
        _localions = '/user/hive/warehouse/profile.db/user_action/' + d
        if fs.exists(_localions):
            print("开始同步时间")
            uup.spark.sql("alter table user_action add partition (dt='%s') location '%s'" % (d, _localions))
    except Exception as e:
        # 已经关联过的异常忽略,partition与hdfs文件不直接关联
        pass
print("同步时间完毕")
uup.spark.sql("add jar /opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/jars/hive-serde-2.1.1-cdh6.3.2.jar;")
uup.spark.sql("add jar /opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/jars/hive-hcatalog-core-2.1.1-cdh6.3.2.jar;")
uup.spark.sql("use profile")
sqlDF = uup.spark.sql(
    "select actionTime, readTime, category_id, param.postId, param.algorithmCombine, param.action, param.userId from user_action where dt>='2021-12-07'")
if sqlDF.collect():
    def _compute(row):
        # 进行判断行为类型
        _list = []
        if row.action == "exposure":
            for post_id in eval(row.postId):
                _list.append(
                    [row.userId, row.actionTime, post_id, row.categoryId, False, False, False, True, row.readTime])
            return _list
        else:
            class Temp(object):
                shared = False
                clicked = False
                collected = False
                read_time = ""

            _tp = Temp()
            if row.action == "share":
                _tp.shared = True
            elif row.action == "click":
                _tp.clicked = True
            elif row.action == "collect":
                _tp.collected = True
            elif row.action == "read":
                _tp.clicked = True
            else:
                pass
            _list.append(
                [int(row.userId), row.actionTime, int(row.postId), row.category_id, _tp.shared, _tp.clicked,
                 _tp.collected,
                 True,
                 row.readTime])
            return _list


    # 进行处理
    # 查询内容，将原始日志表数据进行处理
    _res = sqlDF.rdd.flatMap(_compute)
    data = _res.toDF(
        ["user_id", "action_time", "post_id", "category_id", "shared", "clicked", "collected", "exposure",
         "read_time"])

    # 合并历史数据，插入表中
    old = uup.spark.sql("select * from user_article_basic")
    # 由于合并的结果中不是对于user_id和post_id唯一的，一个用户会对文章多种操作
    new_old = old.unionAll(data)

    new_old.registerTempTable("temptable")
    uup.spark.sql("set hive.enforce.bucketing=false")
    uup.spark.sql("set hive.enforce.sorting=false")
    # 按照用户，文章分组存放进去
    uup.spark.sql(
        "insert overwrite table user_article_basic select user_id, max(action_time) as action_time, post_id, "
        "max(category_id) as category_id, max(shared) as shared, max(clicked) as clicked, max(collected) as "
        "collected, max(exposure) as exposure, max(read_time) as read_time from temptable group by user_id, post_id")

    # 获取基本用户行为信息，然后进行文章画像的主题词合并
    uup.spark.sql("use profile")
    # 取出日志中的category_id
    user_article_ = uup.spark.sql("select * from user_article_basic").drop('category_id')
    uup.spark.sql('use article')
    article_label = uup.spark.sql("select post_id, category_id, topics from article_profile")
    # 合并使用文章中正确的category_id
    click_article_res = user_article_.join(article_label, how='left', on=['post_id'])

    # 将字段的列表爆炸
    import pyspark.sql.functions as F

    click_article_res = click_article_res.withColumn('topic', F.explode('topics')).drop('topics')


    # 计算每个用户对每篇文章的标签的权重
    def compute_weights(rowpartition):
        """处理每个用户对文章的点击数据
        """
        weightsOfaction = {
            "read_min": 1,
            "read_middle": 2,
            "collect": 2,
            "share": 3,
            "click": 5
        }

        import happybase
        from datetime import datetime
        import numpy as np
        #  用于读取hbase缓存结果配置
        pool = happybase.ConnectionPool(size=10, host='hadoop102',
                                        autoconnect=True, transport='framed', protocol='compact')

        # 读取文章的标签数据
        # 计算权重值
        # 时间间隔
        for row in rowpartition:

            t = datetime.now() - datetime.strptime(row.action_time, '%Y-%m-%d %H:%M:%S')
            # 时间衰减系数
            time_exp = 1 / (np.log(t.days + 1) + 1)

            if row.read_time == '':
                r_t = 0
            else:
                r_t = int(row.read_time)
            # 浏览时间分数
            is_read = weightsOfaction['read_middle'] if r_t > 1000 else weightsOfaction['read_min']

            # 每个词的权重分数
            weigths = time_exp * (
                    row.shared * weightsOfaction['share'] + row.collected * weightsOfaction['collect'] + row.
                    clicked * weightsOfaction['click'] + is_read)

            with pool.connection() as conn:
                table = conn.table('user_profile')
                table.put('user:{}'.format(row.user_id).encode(),
                          {'partial:{}:{}'.format(row.category_id, row.topic).encode(): json.dumps(
                              weigths).encode()})
                conn.close()


    click_article_res.foreachPartition(compute_weights)

    """
    更新用户的基础信息画像
    :return:
    """
    uup.spark.sql("use pink")

    user_basic = uup.spark.sql("select user_id, gender, birth from users")


    # 更新用户基础信息
    def _udapte_user_basic(partition):
        """更新用户基本信息
        """
        import happybase
        #  用于读取hbase缓存结果配置
        pool = happybase.ConnectionPool(size=10, host='hadoop102',
                                        autoconnect=True, transport='framed', protocol='compact')
        for row in partition:

            from datetime import date
            age = 0
            if row.birth != 'null':
                born = datetime.strptime(row.birth[0:10], '%Y-%m-%d')
                today = date.today()
                age = today.year - born.year - ((today.month, today.day) < (born.month, born.day))

            with pool.connection() as conn:
                table = conn.table('user_profile')
                table.put('user:{}'.format(row.user_id).encode(),
                          {'basic:gender'.encode(): json.dumps(row.gender).encode()})
                table.put('user:{}'.format(row.user_id).encode(),
                          {'basic:birth'.encode(): json.dumps(age).encode()})
                conn.close()


    user_basic.foreachPartition(_udapte_user_basic)
