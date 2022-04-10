import os
import sys

# 如果当前代码文件运行测试需要加入修改路径，避免出现后导包问题
BASE_DIR = os.path.dirname(os.path.dirname(os.getcwd()))
sys.path.insert(0, os.path.join(BASE_DIR))

PYSPARK_PYTHON = "/miniconda2/envs/reco_sys/bin/python"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
from pyspark import SparkConf
from pyspark.sql import SparkSession
from server.utils import HBaseUtils
from server import pool
from pyspark.ml.linalg import DenseVector
from pyspark.ml.classification import LogisticRegressionModel
import pandas as pd

conf = SparkConf()
config = (
    ("spark.app.name", "sort"),
    ("spark.executor.memory", "2g"),  # 设置该app启动时占用的内存用量，默认1g
    ("spark.master", 'yarn'),
    ("spark.executor.cores", "2"),  # 设置spark executor使用的CPU核心数
)

conf.setAll(config)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

hbu = HBaseUtils(pool)
# 排序
# 1、读取用户特征中心特征
try:
    user_feature = eval(hbu.get_table_row('ctr_feature_user',
                                          '{}'.format(1115629498121846784).encode(),
                                          'category:{}'.format(18).encode()))
except Exception as e:
    user_feature = []

if user_feature:
    # 2、读取文章特征中心特征
    result = []

    for post_id in [17749, 17748, 44371, 44368]:
        try:
            article_feature = eval(hbu.get_table_row('ctr_feature_article',
                                                     '{}'.format(post_id).encode(),
                                                     'article:{}'.format(post_id).encode()))
        except Exception as e:
            article_feature = [0.0] * 111
        f = []
        # 第一个category
        f.extend([article_feature[0]])
        # 第二个article_vector
        f.extend(article_feature[11:])
        # 第三个用户权重特征
        f.extend(user_feature)
        # 第四个文章权重特征
        f.extend(article_feature[1:11])
        vector = DenseVector(f)

        result.append([1115629498121846784, post_id, vector])
# 4、预测并进行排序是筛选
df = pd.DataFrame(result, columns=["user_id", "post_id", "features"])
test = spark.createDataFrame(df)

# 加载逻辑回归模型
model = LogisticRegressionModel.load("hdfs://hadoop102/headlines/models/LR.obj")
predict = model.transform(test)


def vector_to_double(row):
    return float(row.post_id), float(row.probability[1])


res = predict.select(['post_id', 'probability']).rdd.map(vector_to_double).toDF(['post_id', 'probability']).sort(
    'probability', ascending=False)

article_list = [i.post_id for i in res.collect()]
if len(article_list) > 100:
    article_list = article_list[:100]
reco_set = list(map(int, article_list))
