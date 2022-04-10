import os
import sys

# 1.---------article_data
# 如果当前代码文件运行测试需要加入修改路径，避免出现后导包问题
BASE_DIR = os.path.dirname(os.path.dirname(os.getcwd()))
sys.path.insert(0, os.path.join(BASE_DIR))
PYSPARK_PYTHON = "/usr/local/python3"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_181-cloudera"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
from offline import SparkSessionBase


class OriginArticleData(SparkSessionBase):
    SPARK_APP_NAME = "mergeArticle"
    SPARK_URL = "yarn"

    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()


oa = OriginArticleData()
oa.spark.sql("use pink;")
# 由于运行速度原因，选择一篇文章部分数据进行测试
basic_content = oa.spark.sql(
    "select post_id,title,content,category_id from posts")
import pyspark.sql.functions as F
import gc

# 增加category的名字，后面会使用
basic_content.registerTempTable("temptable")
category_basic_content = oa.spark.sql(
    "select t.*, n.category_name from temptable t left join categories n on t.category_id=n.category_id")

# 利用concat_ws方法，将多列数据合并为一个长文本内容（频道，标题以及内容合并）
oa.spark.sql("use article")
sentence_df = category_basic_content.select("post_id", "category_id", "category_name", "title", "content", \
                                           F.concat_ws(
                                               ",",
                                               category_basic_content.category_name,
                                               category_basic_content.title,
                                               category_basic_content.content
                                           ).alias("sentence")
                                           )
del basic_content
del category_basic_content
gc.collect()

sentence_df.write.insertInto("article_data")
