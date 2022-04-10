import os
import sys

# 4.---------user_article_basic,hbase:user_profile
# 如果当前代码文件运行测试需要加入修改路径，否则后面的导包出现问题
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
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
import logging

logger = logging.getLogger('offline')


class UpdateUserProfile(SparkSessionBase):
    """离线相关处理程序
    """
    SPARK_APP_NAME = "updateUser"
    SPARK_URL = "yarn"

    ENABLE_HIVE_SUPPORT = True

    def __init__(self):

        self.spark = self._create_spark_session()

        # 用户文章行为中间表结构
        self._user_article_basic_column = ["user_id", "action_time",
                                           "post_id", "category_id", "shared", "clicked",
                                           "collected", "exposure", "read_time"]

    def update_user_action_basic(self):
        """
        更新用户行为日志到用户行为基础标签表
        :return:
        """
        # 1、读取日志数据进行处理到基础数据表
        self.spark.sql("add jar /opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/jars/hive-serde-2.1.1-cdh6.3.2"
                       ".jar")
        self.spark.sql("use profile")

        # 如果hadoop没有今天该日期文件，则没有日志数据，结束
        time_str = time.strftime("%Y-%m-%d", time.localtime())
        _localions = '/user/hive/warehouse/profile.db/user_action/' + time_str
        fs = pyhdfs.HdfsClient(hosts='hadoop102:9870')
        self.spark.sql("use profile")
        if fs.exists(_localions):
            # 如果有该文件直接关联，捕获关联重复异常
            try:
                self.spark.sql("alter table user_action add partition (dt='%s') location '%s'" % (time_str, _localions))
            except Exception as e:
                pass

            sqlDF = self.spark.sql(
                "select actionTime, readTime, categorySlug, param.postId, param.algorithmCombine, param.action, param.userId from user_action where dt='{}'".format(
                    time_str))

            # 2、判断_df是否存在，存在立即关联目录，建立中间表，插入中间计算结果
            if sqlDF.collect():
                def _compute(row):
                    # 进行判断行为类型
                    _list = []
                    if row.action == "exposure":
                        for post_id in eval(row.postId):
                            _list.append(
                                [row.userId, row.actionTime, post_id, row.categorySlug, False, False, False, True,
                                 row.readTime])
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
                            [row.userId, row.actionTime, int(row.postId), row.categorySlug, _tp.shared, _tp.clicked,
                             _tp.collected, True,
                             row.readTime])
                        return _list

                # 查询内容，将原始日志表数据进行处理
                _res = sqlDF.rdd.flatMap(_compute)
                data = _res.toDF(self._user_article_basic_column)

                # 将得到的每个用户对每篇文章的日志结果合并，写入到用户与文章基础行为信息表中
                old = self.spark.sql("select * from user_article_basic")
                self.spark.sql("set hive.enforce.bucketing=false")
                self.spark.sql("set hive.enforce.sorting=false")
                old.unionAll(data).registerTempTable("temptable")
                # 直接写入用户与文章基础行为表
                self.spark.sql(
                    "insert overwrite table user_article_basic select user_id, max(action_time) as action_time, "
                    "post_id, max(category_id) as category_id, max(shared) as shared, max(clicked) as clicked, "
                    "max(collected) as collected, max(exposure) as exposure, max(read_time) as read_time from "
                    "temptable "
                    "group by user_id, post_id")

                logger.info(
                    "{} INFO completely update user_article_basic".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                return True
            return False
        else:
            return False

    def update_user_label(self):
        # 3、查询用户与文章基础行为信息表结果，合并每个频道的主题词中间表，与中间表关联
        # result = self.spark.sql("select * from user_article_basic order by post_id")
        # 对每个主题下的主题词进行合并

        # 制作每个人的字典对应每个标签的分值，计算每个用户的变迁权重公式：
        user_article_ = self.spark.sql("select * from user_article_basic").drop('category_id')
        self.spark.sql('use article')
        article_label = self.spark.sql("select post_id, category_id, topics from article_profile")
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

            from datetime import datetime
            import numpy as np
            import json
            import happybase
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
        logger.info(
            "{} INFO completely update user weights of label".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    def update_user_info(self):
        """
        更新用户的基础信息画像
        :return:
        """
        self.spark.sql("use pink")

        user_basic = self.spark.sql("select user_id, gender, birth from users")

        # 更新用户基础信息
        def _udapte_user_basic(partition):
            """更新用户基本信息
            """
            from datetime import datetime
            import json
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
        logger.info(
            "{} INFO completely update infomation of basic".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))


if __name__ == '__main__':
    op = UpdateUserProfile()
    op.update_user_action_basic()
    op.update_user_label()
    op.update_user_info()
