from pyspark import SparkConf
from pyspark.sql import SparkSession


# 初始化spark
class SparkSessionBase(object):
    SPARK_APP_NAME = None
    SPARK_URL = "yarn"

    SPARK_EXECUTOR_MEMORY = "3g"
    SPARK_EXECUTOR_CORES = 2
    SPARK_EXECUTOR_INSTANCES = 4
    SPARK_MESSAGE_MAXSIZE = 1024

    ENABLE_HIVE_SUPPORT = False

    def _create_spark_session(self):

        conf = SparkConf()  # 创建spark config对象
        config = (
            ("spark.app.name", self.SPARK_APP_NAME),  # 设置启动的spark的app名称，没有提供，将随机产生一个名称
            ("spark.executor.memory", self.SPARK_EXECUTOR_MEMORY),  # 设置该app启动时占用的内存用量，默认2g
            ("spark.master", self.SPARK_URL),  # spark master的地址
            ("spark.executor.cores", self.SPARK_EXECUTOR_CORES),  # 设置spark executor使用的CPU核心数，默认是1核心
            ("spark.executor.instances", self.SPARK_EXECUTOR_INSTANCES),
            ("spark.rpc.message.maxSize", self.SPARK_MESSAGE_MAXSIZE),
            ("yarn.scheduler.maximum-allocation-mb", "20g"),
        )

        conf.setAll(config)

        # 利用config对象，创建spark session
        if self.ENABLE_HIVE_SUPPORT:
            return SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
        else:
            return SparkSession.builder.config(conf=conf).getOrCreate()

    def _create_spark_hbase(self):

        conf = SparkConf()
        config = (
            ("spark.app.name", self.SPARK_APP_NAME),
            ("spark.executor.memory", self.SPARK_EXECUTOR_MEMORY),
            ("spark.master", self.SPARK_URL),
            ("spark.executor.cores", self.SPARK_EXECUTOR_CORES),
            ("spark.executor.instances", self.SPARK_EXECUTOR_INSTANCES),
            ("spark.rpc.message.maxSize", self.SPARK_MESSAGE_MAXSIZE),
            ("hbase.zookeeper.quorum", "hadoop102"),
            ("hbase.zookeeper.property.clientPort", "2181"),
            ("yarn.scheduler.maximum-allocation-mb", "20g"),
        )

        conf.setAll(config)

        if self.ENABLE_HIVE_SUPPORT:
            return SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
        else:
            return SparkSession.builder.config(conf=conf).getOrCreate()
