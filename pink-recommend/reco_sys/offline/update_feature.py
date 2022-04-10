import os
import sys

# 如果当前代码文件运行测试需要加入修改路径，否则后面的导包出现问题
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
PYSPARK_PYTHON = "/usr/local/python3/bin/python3"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_181-cloudera"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON

import findspark

findspark.init("/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark")
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel
from offline import SparkSessionBase

import happybase
import pyspark.sql.functions as F
from datetime import datetime
from datetime import timedelta
import time
import gc


class FeaturePlatform(SparkSessionBase):
    """特征更新平台
    """
    SPARK_APP_NAME = "featureCenter"
    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        # _create_spark_session
        # _create_spark_hbase用户spark sql 操作hive对hbase的外部表
        self.spark = self._create_spark_hbase()

    def update_user_ctr_feature_to_hbase(self):
        """
        :return:
        """
        global StructType, StructField, MapType, StringType, DoubleType, LongType
        self.spark.sql("use profile")

        user_profile_hbase = self.spark.sql(
            "select user_id, information['birth'], information['gender'], article_partial, env from user_profile_hbase")
        user_profile_hbase.show()
        # 特征工程处理
        # 抛弃获取值少的特征
        user_profile_hbase = user_profile_hbase.drop('env', 'birth', 'gender')

        def get_user_id(row):
            return int(row.user_id.split(":")[1]), row.article_partial

        user_profile_hbase.show()
        user_profile_hbase_temp = user_profile_hbase.rdd.map(get_user_id)

        _schema = StructType([
            StructField("user_id", LongType()),
            StructField("weights", MapType(StringType(), DoubleType()))
        ])

        user_profile_hbase_schema = self.spark.createDataFrame(user_profile_hbase_temp, schema=_schema)

        def frature_preprocess(row):

            from pyspark.ml.linalg import Vectors

            category_weights = []
            for i in range(1, 26):
                try:
                    _res = sorted([row.weights[key] for key
                                   in row.weights.keys() if key.split(':')[0] == str(i)])[:3]
                    category_weights.append(_res)
                except:
                    category_weights.append([])

            return row.user_id, category_weights

        res = user_profile_hbase_schema.rdd.map(frature_preprocess).collect()

        # 批量插入Hbase数据库中
        pool = happybase.ConnectionPool(size=10, host='hadoop102',
                                        autoconnect=True, transport='framed', protocol='compact')
        with pool.connection() as conn:
            ctr_feature = conn.table('ctr_feature_user')
            with ctr_feature.batch(transaction=True) as b:
                for i in range(len(res)):
                    for j in range(25):
                        b.put("{}".format(res[i][0]).encode(),
                              {"category:{}".format(j + 1).encode(): str(res[i][1][j]).encode()})
            conn.close()

    def update_article_ctr_feature_to_hbase(self):
        """
        :return:
        """
        # 文章特征中心
        self.spark.sql("use article")
        article_profile = self.spark.sql("select * from article_profile")

        def article_profile_to_feature(row):
            try:
                weights = sorted(row.keywords.values())[:3]
            except Exception as e:
                weights = [0.0] * 3
            return row.post_id, row.category_id, weights

        article_profile = article_profile.rdd.map(article_profile_to_feature).toDF(
            ['post_id', 'category_id', 'weights'])

        article_vector = self.spark.sql("select * from article_vector")
        article_feature = article_profile.join(article_vector, on=['post_id'], how='inner')

        def feature_to_vector(row):
            from pyspark.ml.linalg import Vectors
            return row.post_id, row.category_id, Vectors.dense(row.weights), Vectors.dense(row.articlevector)

        article_feature = article_feature.rdd.map(feature_to_vector).toDF(
            ['post_id', 'category_id', 'weights', 'articlevector'])

        # 保存特征数据
        cols2 = ['post_id', 'category_id', 'weights', 'articlevector']
        # 做特征的指定指定合并
        article_feature_two = VectorAssembler().setInputCols(cols2[1:4]).setOutputCol("features").transform(
            article_feature)

        # 保存到特征数据库中
        def save_article_feature_to_hbase(partition):
            import happybase
            pool = happybase.ConnectionPool(size=10, host='hadoop102',
                                            autoconnect=True, transport='framed', protocol='compact')
            with pool.connection() as conn:
                table = conn.table('ctr_feature_article')
                for row in partition:
                    table.put('{}'.format(row.post_id).encode(),
                              {'article:{}'.format(row.post_id).encode(): str(row.features).encode()})

        article_feature_two.foreachPartition(save_article_feature_to_hbase)


if __name__ == '__main__':
    fp = FeaturePlatform()
    fp.update_user_ctr_feature_to_hbase()
    fp.update_article_ctr_feature_to_hbase()
