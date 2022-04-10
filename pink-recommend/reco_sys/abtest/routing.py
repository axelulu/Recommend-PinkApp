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
from concurrent import futures
from abtest import user_reco_pb2
from abtest import user_reco_pb2_grpc
from setting.default import DefaultConfig, RAParam
from server.reco_center import RecoCenter
import grpc
import hashlib
import time
import json
import setting.logging as lg


def add_track(res, temp):
    """
    封装埋点参数
    :param res: 推荐文章id列表
    :param cb: 合并参数
    :param rpc_param: rpc参数
    :return: 埋点参数
        文章列表参数
        单文章参数
    """
    # 添加埋点参数
    track = {}

    # 准备曝光参数
    # 全部字符串形式提供，在hive端不会解析问题
    _exposure = {"action": "exposure", "userId": temp.user_id, "postId": json.dumps(res),
                 "algorithmCombine": temp.algo}

    track['param'] = json.dumps(_exposure)
    track['recommends'] = []

    # 准备其它点击参数
    for _id in res:
        # 构造字典
        _dic = {}
        _dic['post_id'] = _id
        _dic['param'] = {}

        # 准备click参数
        _p = {"action": "click", "userId": temp.user_id, "postId": str(_id),
              "algorithmCombine": temp.algo}

        _dic['param']['click'] = json.dumps(_p)
        # 准备collect参数
        _p["action"] = 'collect'
        _dic['param']['collect'] = json.dumps(_p)
        # 准备share参数
        _p["action"] = 'share'
        _dic['param']['share'] = json.dumps(_p)
        # 准备detentionTime参数
        _p["action"] = 'read'
        _dic['param']['read'] = json.dumps(_p)

        track['recommends'].append(_dic)

    track['timestamp'] = temp.time_stamp
    return track


def feed_recommend(user_id, category_id, article_num, time_stamp):
    """
    1、根据web提供的参数，进行分流
    2、找到对应的算法组合之后，去推荐中心调用不同的召回和排序服务
    3、进行埋点参数封装
    :param user_id:用户id
    :param article_num:推荐文章个数
    :return: track:埋点参数结果: 参考上面埋点参数组合
    """

    #  产品前期推荐由于较少的点击行为，所以去做 用户冷启动 + 文章冷启动
    # 用户冷启动：'推荐'频道：热门频道的召回+用户实时行为画像召回（在线的不保存画像）  'C2'组合
    #            # 其它频道：热门召回 + 新文章召回   'C1'组合
    # 定义返回参数的类
    class TempParam(object):
        user_id = -10
        category_id = -10
        article_num = -10
        time_stamp = -10
        algo = ""

    temp = TempParam()
    temp.user_id = user_id
    temp.category_id = category_id
    temp.article_num = article_num
    # 请求的时间戳大小
    temp.time_stamp = time_stamp

    # 先读取缓存数据redis+待推荐hbase结果
    # 如果有返回并加上埋点参数
    # 并且写入hbase 当前推荐时间戳用户（登录和匿名）的历史推荐文章列表

    # 传入用户id为空的直接召回结果
    if temp.user_id == "":
        temp.algo = ""
        return add_track([], temp)
    # 进行分桶实现分流，制定不同的实验策略
    bucket = hashlib.md5(user_id.encode()).hexdigest()[:1]
    if bucket in RAParam.BYPASS[0]['Bucket']:
        temp.algo = RAParam.BYPASS[0]['Strategy']
    else:
        temp.algo = RAParam.BYPASS[1]['Strategy']

    # 推荐服务中心推荐结果(这里做测试)
    # track = add_track([], temp)
    track = RecoCenter().feed_recommend_logic(temp)

    return track


# 基于用户推荐的rpc服务推荐
# 定义指定的rpc服务输入输出参数格式proto
class UserRecommendServicer(user_reco_pb2_grpc.UserRecommendServicer):
    """
    对用户进行技术文章推荐
    """

    def user_recommend(self, request, context):
        """
        用户feed流推荐
        :param request:
        :param context:
        :return:
        """
        # 选择C4组合
        user_id = request.user_id
        category_id = request.category_id
        article_num = request.article_num
        time_stamp = request.time_stamp

        # 解析参数，并进行推荐中心推荐(暂时使用假数据替代)
        _track = feed_recommend(user_id, category_id, article_num, time_stamp)

        # 解析返回参数到rpc结果参数
        # 参数如下
        # [       {"post_id": 1, "param": {"click": "", "collect": "", "share": "", 'detentionTime':''}},
        #         {"post_id": 2, "param": {"click": "", "collect": "", "share": "", 'detentionTime':''}},
        #         {"post_id": 3, "param": {"click": "", "collect": "", "share": "", 'detentionTime':''}},
        #         {"post_id": 4, "param": {"click": "", "collect": "", "share": "", 'detentionTime':''}}
        #     ]
        # 第二个rpc参数
        _param1 = []
        for _ in _track['recommends']:
            # param的封装
            _params = user_reco_pb2.param2(click=_['param']['click'],
                                           collect=_['param']['collect'],
                                           share=_['param']['share'],
                                           read=_['param']['read'])
            _p2 = user_reco_pb2.param1(post_id=_['post_id'], params=_params)
            _param1.append(_p2)
        # param
        return user_reco_pb2.Track(exposure=_track['param'], recommends=_param1, time_stamp=_track['timestamp'])


#    def article_recommend(self, request, context):
#        """
#       文章相似推荐
#       :param request:
#       :param context:
#       :return:
#       """
#       # 获取web参数
#       post_id = request.post_id
#       article_num = request.article_num
#
#        # 进行文章相似推荐,调用推荐中心的文章相似
#       _article_list = article_reco_list(post_id, article_num, 105)
#
#       # rpc参数封装
#       return user_reco_pb2.Similar(post_id=_article_list)


MAX_MESSAGE_LENGTH = 256 * 1024 * 1024  # 设置grpc最大可接受的文件大小 256M


def serve():
    # 创建recommend日志
    lg.create_logger()
    # 多线程服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10),
                         options=[('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
                                  ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH)])
    # 注册本地服务
    user_reco_pb2_grpc.add_UserRecommendServicer_to_server(UserRecommendServicer(), server)
    # 监听端口
    server.add_insecure_port(DefaultConfig.RPC_SERVER)

    # 开始接收请求进行服务
    print("开始监听")
    server.start()
    # 使用 ctrl+c 可以退出服务
    _ONE_DAY_IN_SECONDS = 60 * 60 * 24
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    serve()
