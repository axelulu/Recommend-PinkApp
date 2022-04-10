import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
PYSPARK_PYTHON = "/usr/local/python3/bin/python3"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_181-cloudera"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
# os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
import happybase
from setting.default import DefaultConfig
import redis

pool = happybase.ConnectionPool(size=10, host='hadoop102',
                                autoconnect=True, transport='framed', protocol='compact')

# 召回数据
redis_client = redis.StrictRedis(host=DefaultConfig.REDIS_HOST,
                                 port=DefaultConfig.REDIS_PORT,
                                 db=10,
                                 decode_responses=True)
# 用于缓存的Redis数据库
cache_client = redis.StrictRedis(host=DefaultConfig.REDIS_HOST,
                                 port=DefaultConfig.REDIS_PORT,
                                 db=8,
                                 decode_responses=True)

from pyspark import SparkConf
from pyspark.sql import SparkSession

# spark配置
conf = SparkConf()
conf.setAll(DefaultConfig.SPARK_GRPC_CONFIG)

SORT_SPARK = SparkSession.builder.config(conf=conf).getOrCreate()
