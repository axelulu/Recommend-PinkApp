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
import findspark

findspark.init("/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark")
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from setting.default import DefaultConfig

import happybase

#  用于读取hbase缓存结果配置
pool = happybase.ConnectionPool(size=10, host='hadoop102',
                                autoconnect=True, transport='framed', protocol='compact')
# 1、创建conf
conf = SparkConf()
conf.setAll(DefaultConfig.SPARK_ONLINE_CONFIG)
# 建立spark session以及spark streaming context
sc = SparkContext(conf=conf)
# 创建Streaming Context
stream_c = StreamingContext(sc, 60)

# 基于内容召回配置，用于收集用户行为，获取相似文章实时推荐
similar_kafkaParams = {"metadata.broker.list": DefaultConfig.KAFKA_SERVER, "group.id": 'similar'}
SIMILAR_DS = KafkaUtils.createDirectStream(stream_c, ['click-trace'], similar_kafkaParams)

# 添加sparkstreaming启动对接kafka的配置
# 配置KAFKA相关，用于热门文章KAFKA读取
click_kafkaParams = {"metadata.broker.list": DefaultConfig.KAFKA_SERVER}
HOT_DS = KafkaUtils.createDirectStream(stream_c, ['click-trace'], click_kafkaParams)

# new-article，新文章的读取  KAFKA配置
NEW_ARTICLE_DS = KafkaUtils.createDirectStream(stream_c, ['click-trace'], click_kafkaParams)
