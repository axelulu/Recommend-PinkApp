import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
sys.path.insert(0, os.path.join(BASE_DIR, 'reco_sys'))
PYSPARK_PYTHON = "/usr/local/python3/bin/python3"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_181-cloudera"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
from scheduler.update import update_article_profile
from scheduler.update import update_user_profile
from scheduler.update import update_recall
from scheduler.update import update_ctr_feature
import setting.logging as lg

lg.create_logger()

# 创建scheduler，多进程执行
executors = {
    'default': ProcessPoolExecutor(3)
}

scheduler = BlockingScheduler(executors=executors)

# 添加定时更新任务更新文章画像,每隔一小时更新
scheduler.add_job(update_article_profile, trigger='interval', hours=1)
# 添加定时更新任务更新用户画像,每隔二小时更新
scheduler.add_job(update_user_profile, trigger='interval', hours=2)
# 添加定时更新任务更新用户召回信息,每隔三小时更新
scheduler.add_job(update_recall, trigger='interval', hours=3)
# 添加定时更新用户文章特征结果的程序，每个4小时更新一次
scheduler.add_job(update_ctr_feature, trigger='interval', hours=4)
scheduler.start()
