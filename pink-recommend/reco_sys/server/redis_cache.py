from server import cache_client
import logging
from datetime import datetime

logger = logging.getLogger('recommend')


def get_cache_from_redis_hbase(temp, hbu):
    """
    进行用户频道推荐缓存结果的读取
    :param temp: 用户请求的参数
    :param hbu: hbase工具
    :return:
    """

    # - 首先从获取redis结果，进行判断(缓存拿)
    key = 'reco:{}:{}:art'.format(temp.user_id, temp.category_id)
    res = cache_client.zrevrange(key, 0, temp.article_num - 1)
    # - 如果redis有，读取需要推荐的文章数量放回，并删除这些文章，并且放入推荐历史推荐结果中
    if res:
        cache_client.zrem(key, *res)
    else:
        # - 如果redis当中不存在，则从wait_recommend中读取
        # 删除redis这个键
        cache_client.delete(key)
        try:
            # 字符串编程列表
            hbase_cache = eval(hbu.get_table_row('wait_recommend',
                                                 'reco:{}'.format(temp.user_id).encode(),
                                                 'category:{}'.format(temp.category_id).encode()))

        except Exception as e:
            logger.warning("{} WARN read user_id:{} wait_recommend exception:{} not exist".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, e))

            hbase_cache = []
        if not hbase_cache:
            #   - 如果wait_recommend中也没有，直接返回空，去进行一次召回读取、排序然后推荐
            return hbase_cache
        #   - wait_recommend有，从wait_recommend取出所有结果，定一个数量(如100篇)的文章存入redis
        if len(hbase_cache) > 100:
            logger.info(
                "{} INFO reduce user_id:{} category:{} wait_recommend data".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.category_id))
            # 拿出固定数量（100）给redis
            cache_redis = hbase_cache[:100]
            # 放入redis缓存
            cache_client.zadd(key, dict(zip(cache_redis, range(len(cache_redis)))))
            # 剩下的放回wait hbase结果
            hbu.get_table_put('wait_recommend',
                              'reco:{}'.format(temp.user_id).encode(),
                              'category:{}'.format(temp.category_id).encode(),
                              str(hbase_cache[100:]).encode())
        else:
            logger.info(
                "{} INFO delete user_id:{} category:{} wait_recommend data".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.category_id))
            # - wait_recommend不够一定数量，全部取出放入redis当中，直接推荐出去
            # 清空wait_recommend
            hbu.get_table_put('wait_recommend',
                              'reco:{}'.format(temp.user_id).encode(),
                              'category:{}'.format(temp.category_id).encode(),
                              str([]).encode())

            # 放入redis缓存
            cache_client.zadd(key, dict(zip(hbase_cache, range(len(hbase_cache)))))

        res = cache_client.zrevrange(key, 0, temp.article_num - 1)
        if res:
            cache_client.zrem(key, *res)

    # 进行执行PL，然后写入历史推荐结果
    # 进行类型转换
    res = list(map(int, res))
    logger.info("{} INFO get cache data and store user_id:{} category:{} cache history data".format(
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.category_id))

    # 放入历史记录
    hbu.get_table_put('history_recommend',
                      'reco:his:{}'.format(temp.user_id).encode(),
                      'category:{}'.format(temp.category_id).encode(),
                      str(res).encode(),
                      timestamp=temp.time_stamp)

    return res
