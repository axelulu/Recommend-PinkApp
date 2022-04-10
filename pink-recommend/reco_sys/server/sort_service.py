from server import SORT_SPARK
from pyspark.ml.linalg import DenseVector
from pyspark.ml.classification import LogisticRegressionModel
import pandas as pd
import numpy as np
from datetime import datetime
import logging

logger = logging.getLogger("recommend")


def lr_sort_service(reco_set, temp, hbu):
    """
    排序返回推荐文章
    :param reco_set:召回合并过滤后的结果
    :param temp: 参数
    :param hbu: Hbase工具
    :return:
    """
    # 排序
    # 1、读取用户特征中心特征
    try:
        user_feature = eval(hbu.get_table_row('ctr_feature_user',
                                              '{}'.format(temp.user_id).encode(),
                                              'category:{}'.format(temp.category_id).encode()))
        logger.info("{} INFO get user user_id:{} category:{} profile data".format(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.category_id))
    except Exception as e:
        user_feature = []
    print(reco_set)
    if user_feature:
        # 2、读取文章特征中心特征
        result = []

        for post_id in reco_set:
            try:
                article_feature = eval(hbu.get_table_row('ctr_feature_article',
                                                         '{}'.format(post_id).encode(),
                                                         'article:{}'.format(post_id).encode()))
            except Exception as e:

                article_feature = [0.0] * 111
            f = []
            # 第一个category_id
            f.extend([article_feature[0]])
            # 第二个article_vector
            f.extend(article_feature[11:])
            # 第三个用户权重特征
            f.extend(user_feature)
            # 第四个文章权重特征
            f.extend(article_feature[1:11])
            vector = DenseVector(f)
            result.append([temp.user_id, post_id, vector])
        # 4、预测并进行排序是筛选
        df = pd.DataFrame(result, columns=["user_id", "post_id", "features"])
        test = SORT_SPARK.createDataFrame(df)

        # 加载逻辑回归模型
        model = LogisticRegressionModel.load("hdfs://hadoop102/headlines/models/logistic_ctr_model.obj")
        predict = model.transform(test)
        predict.show()

        def vector_to_double(row):
            return float(row.post_id), float(row.probability[1])

        res = predict.select(['post_id', 'probability']).rdd.map(vector_to_double).toDF(
            ['post_id', 'probability']).sort('probability', ascending=False)
        article_list = [i.post_id for i in res.collect()]
        logger.info("{} INFO sorting user_id:{} recommend article".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                          temp.user_id))
        # 排序后，只将排名在前100个文章ID返回给用户推荐
        if len(article_list) > 100:
            article_list = article_list[:100]
        reco_set = list(map(int, article_list))

    return reco_set
