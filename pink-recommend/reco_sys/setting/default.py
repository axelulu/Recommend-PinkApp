# 增加spark online 启动配置
class DefaultConfig(object):
    """默认的一些配置信息
    """
    SPARK_ONLINE_CONFIG = (
        ("spark.app.name", "onlineUpdate"),  # 设置启动的spark的app名称，没有提供，将随机产生一个名称
        ("spark.master", "yarn"),
        ("spark.executor.instances", 4),
        ("spark.ui.port", 4040)
    )
    # KAFKA配置
    KAFKA_SERVER = "hadoop103:9092"

    # redis的IP和端口配置
    REDIS_HOST = "hadoop102"
    REDIS_PORT = 6379

    # rpc
    RPC_SERVER = 'hadoop102:9898'

    # 实时运行spark
    # SPARK grpc配置
    SPARK_GRPC_CONFIG = (
        ("spark.app.name", "grpcSort"),  # 设置启动的spark的app名称，没有提供，将随机产生一个名称
        ("spark.master", "yarn"),
        ("spark.executor.instances", 4),
        ("spark.ui.port", 4042)
    )


from collections import namedtuple

# abtest参数信息
# ABTest参数
param = namedtuple('RecommendAlgorithm', ['COMBINE',
                                          'RECALL',
                                          'SORT',
                                          'CATEGORY',
                                          'BYPASS']
                   )

RAParam = param(
    COMBINE={
        'Algo-1': (1, [100, 101, 102, 103, 104], [200]),  # 首页推荐，所有召回结果读取+LR排序
        'Algo-2': (2, [100, 101, 102, 103, 104], [200])  # 首页推荐，所有召回结果读取 排序
    },
    RECALL={
        100: ('cb_recall', 'als'),  # 离线模型ALS召回，recall:user:1115629498121 column=als:18
        101: ('cb_recall', 'content'),  # 离线word2vec的画像内容召回 'recall:user:5', 'content:1'
        102: ('cb_recall', 'online'),  # 在线word2vec的画像召回 'recall:user:1', 'online:1'
        103: 'new_article',  # 新文章召回 redis当中    ch:18:new
        104: 'popular_article',  # 基于用户协同召回结果 ch:18:hot
        105: ('article_similar', 'similar')  # 文章相似推荐结果 '1' 'similar:2'
    },
    SORT={
        200: 'LR',
    },
    CATEGORY="9",
    BYPASS=[
        {
            "Bucket": ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd'],
            "Strategy": "Algo-1"
        },
        {
            "BeginBucket": ['e', 'f'],
            "Strategy": "Algo-2"
        }
    ]
)

# 分类信息
categoryInfo = {
    "1": "film",
    "2": "game",
    "3": "anime",
    "4": "music",
    "5": "novel",
    "6": "comic",
    "7": "new",
    "8": "tech",
    "9": "tv",
}
