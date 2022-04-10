import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
PYSPARK_PYTHON = "/usr/local/python3/bin/python3"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_181-cloudera"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
from abtest import user_reco_pb2_grpc
from abtest import user_reco_pb2
import grpc
from setting.default import DefaultConfig
import time


def test():
    article_dict = {}
    # 构造传入数据

    req_article = user_reco_pb2.User()
    req_article.user_id = '2079499053372018688'
    req_article.category_id = 2
    req_article.article_num = 10
    # req_article.time_stamp = int(time.time() * 1000)
    req_article.time_stamp = 1649412155013

    with grpc.insecure_channel(DefaultConfig.RPC_SERVER) as rpc_cli:
        try:
            stub = user_reco_pb2_grpc.UserRecommendStub(rpc_cli)
            resp = stub.user_recommend(req_article)
        except Exception as e:
            print(e)
            article_dict['param'] = []
        else:

            # 解析返回结果参数
            article_dict['exposure_param'] = resp.exposure

            reco_arts = resp.recommends

            reco_art_param = []
            reco_list = []
            for art in reco_arts:
                reco_art_param.append({
                    'post_id': art.post_id,
                    'params': {
                        'click': art.params.click,
                        'collect': art.params.collect,
                        'share': art.params.share,
                        'read': art.params.read
                    }
                })

                reco_list.append(art.post_id)
            article_dict['param'] = reco_art_param

            # 文章列表以及参数（曝光参数 以及 每篇文章的点击等参数）
            print(reco_list, article_dict)


if __name__ == '__main__':
    test()
