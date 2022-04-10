import os
import sys
import codecs

# 6.---------ctr_feature_user
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

BASE_DIR = os.path.dirname(os.path.dirname(os.getcwd()))
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


# 初始化spark
class CtrLogisticRegression(SparkSessionBase):
    SPARK_APP_NAME = "ctrLogisticRegression"
    SPARK_URL = "yarn"

    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()


ctr = CtrLogisticRegression()

ctr.spark.sql("use profile")
# +-------------------+----------+----------+-------+
# |            user_id|post_id|category_id|clicked|
# +-------------------+----------+----------+-------+
# |1105045287866466304|     14225|         0|  false|
user_article_basic = ctr.spark.sql("select * from user_article_basic").select(
    ['user_id', 'post_id', 'clicked'])
user_profile_hbase = ctr.spark.sql(
    "select user_id, information.gender, information.birth, article_partial from user_profile_hbase")


# 对于用户ID做一个处理，取出前面的user字符串
def deal_with_user_id(row):
    return int(row.user_id.split(':')[1]), row.gender, row.birth, row.article_partial

user_profile_hbase.show()
user_profile = user_profile_hbase.rdd.map(deal_with_user_id)

# +--------------------+--------+------+--------------------+
# |             user_id|birth|gender|     article_partial|
# +--------------------+--------+------+--------------------+
# |              user:1|     0.0|  null|Map(18:Animal -> ...|

_schema = StructType([
    StructField('user_id', LongType()),
    StructField('gender', DoubleType()),
    StructField('birth', DoubleType()),
    StructField('article_partial', MapType(StringType(), DoubleType()))
])
user_profile_hbase = ctr.spark.createDataFrame(user_profile, schema=_schema).drop('gender').drop('birth')
user_profile_hbase.show()
train = user_article_basic.join(user_profile_hbase, on=['user_id'], how='left')
# +-------------------+----------+-------+--------+------+--------------------+
# |            user_id|post_id|clicked|birth|gender|             weights|
# +-------------------+----------+-------+--------+------+--------------------+
# |1106473203766657024|     13778|  false|     0.0|  null|Map(18:text -> 0....|
ctr.spark.sql("use article")
article_vector = ctr.spark.sql("select * from article_vector")
train = train.join(article_vector, on=['post_id'], how='left')
# +-------------------+-------------------+-------+--------------------+----------+--------------------+
# |         post_id|            user_id|clicked|             weights|category_id|       articlevector|
# +-------------------+-------------------+-------+--------------------+----------+--------------------+
# |              13401|                 10|  false|Map(18:tp2 -> 0.2...|        18|[0.06157120217893...|

ctr.spark.sql("use article")
article_profile = ctr.spark.sql("select post_id, keywords from article_profile")


def article_profile_to_feature(row):
    try:
        weights = sorted(row.keywords.values())[:3]
    except Exception as e:
        weights = [0.0] * 3
    return row.post_id, weights


article_profile = article_profile.rdd.map(article_profile_to_feature).toDF(['post_id', 'article_weights'])

train2 = train.join(article_profile, on=['post_id'], how='left')
# - (4)进行用户的权重特征筛选处理，类型处理
train = train2.dropna()

columns = ['post_id', 'user_id', 'category_id', 'articlevector', 'user_weights', 'article_weights', 'clicked']


# array --->vecoter
def get_user_weights(row):
    # 取出所有对应particle平道的关键词权重（用户）
    from pyspark.ml.linalg import Vectors
    try:
        weights = sorted([row.article_partial[key] for key in
                          row.article_partial.keys() if key.split(':')[0] == str(row.category_id)])[:3]
    except Exception as e:
        weights = [0.0] * 3

    return row.post_id, row.user_id, row.category_id, Vectors.dense(row.articlevector), Vectors.dense(
        weights), Vectors.dense(row.article_weights), int(row.clicked)


train_1 = train.rdd.map(get_user_weights).toDF(columns)
train_version_two = VectorAssembler().setInputCols(columns[2:6]).setOutputCol(
    "features").transform(
    train_1)
lr = LogisticRegression()
model = lr.setLabelCol("clicked").setFeaturesCol("features").fit(train_version_two)
model.save("hdfs://hadoop102/headlines/models/logistic_ctr_model.obj")

online_model = LogisticRegressionModel.load("hdfs://hadoop102/headlines/models/logistic_ctr_model.obj")

res_transfrom = online_model.transform(train_version_two)
res_transfrom.show()

res_transfrom.select(["clicked", "probability", "prediction"]).show()


def vector_to_double(row):
    return float(row.clicked), float(row.probability[1])


score_label = res_transfrom.select(["clicked", "probability"]).rdd.map(vector_to_double)

# 构造样本
ctr.spark.sql("use profile")

user_profile_hbase = ctr.spark.sql(
    "select user_id, information.gender, information.birth, article_partial, env from user_profile_hbase")

# 特征工程处理
# 抛弃获取值少的特征
user_profile_hbase = user_profile_hbase.drop('env', 'birth', 'gender')


def get_user_id(row):
    return int(row.user_id.split(":")[1]), row.article_partial


user_profile_hbase_temp = user_profile_hbase.rdd.map(get_user_id)

from pyspark.sql.types import *

_schema = StructType([
    StructField("user_id", LongType()),
    StructField("weights", MapType(StringType(), DoubleType()))
])

user_profile_hbase_schema = ctr.spark.createDataFrame(user_profile_hbase_temp, schema=_schema)


def frature_preprocess(row):
    from pyspark.ml.linalg import Vectors

    category_weights = []
    for i in range(1, 26):
        try:
            _res = sorted([row.weights[key] for key
                           in row.weights.keys() if key.split(':')[0] == str(i)])[:10]
            category_weights.append(_res)
        except:
            category_weights.append([0.0] * 10)

    return row.user_id, category_weights


res = user_profile_hbase_schema.rdd.map(frature_preprocess).collect()

import happybase

# 批量插入Hbase数据库中
pool = happybase.ConnectionPool(size=10, host='hadoop102',
                                autoconnect=True, transport='framed', protocol='compact')
with pool.connection() as conn:
    ctr_feature_user = conn.table('ctr_feature_user')
    with ctr_feature_user.batch(transaction=True) as b:
        for i in range(len(res)):
            for j in range(25):
                # j 0~~~24
                b.put('{}'.format(res[i][0]).encode(),
                      {'category:{}'.format(j + 1).encode(): str(res[i][1][j]).encode()})
    conn.close()
