import os
import sys

from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.ml.linalg import Vectors

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
PYSPARK_PYTHON = "/usr/local/python3/bin/python3"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_181-cloudera"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
from offline import SparkSessionBase
from datetime import datetime
from datetime import timedelta
import pyspark.sql.functions as F
import pyspark
import gc
import logging
from setting.default import categoryInfo

logger = logging.getLogger('offline')


# 分词
def segmentation(partition):
    import os
    import re

    import jieba
    import jieba.analyse
    import jieba.posseg as pseg
    import codecs

    abspath = "/bigdata/words"

    # 结巴加载用户词典
    userDict_path = os.path.join(abspath, "ITKeywords.txt")
    jieba.load_userdict(userDict_path)

    # 停用词文本
    stopwords_path = os.path.join(abspath, "stopwords.txt")

    def get_stopwords_list():
        """返回stopwords列表"""
        stopwords_list = [i.strip()
                          for i in codecs.open(stopwords_path).readlines()]
        return stopwords_list

    # 所有的停用词列表
    stopwords_list = get_stopwords_list()

    # 分词
    def cut_sentence(sentence):
        """对切割之后的词语进行过滤，去除停用词，保留名词，英文和自定义词库中的词，长度大于2的词"""
        # print(sentence,"*"*100)
        # eg:[pair('今天', 't'), pair('有', 'd'), pair('雾', 'n'), pair('霾', 'g')]
        seg_list = pseg.lcut(sentence)
        seg_list = [i for i in seg_list if i.flag not in stopwords_list]
        filtered_words_list = []
        for seg in seg_list:
            # print(seg)
            if len(seg.word) <= 1:
                continue
            elif seg.flag == "eng":
                if len(seg.word) <= 2:
                    continue
                else:
                    filtered_words_list.append(seg.word)
            elif seg.flag.startswith("n"):
                filtered_words_list.append(seg.word)
            elif seg.flag in ["x", "eng"]:  # 是自定一个词语或者是英文单词
                filtered_words_list.append(seg.word)
        return filtered_words_list

    for row in partition:
        sentence = re.sub("<.*?>", "", row.sentence)  # 替换掉标签数据
        words = cut_sentence(sentence)
        yield row.post_id, row.category_id, words


# 分词
def textrank(partition):
    import os

    import jieba
    import jieba.analyse
    import jieba.posseg as pseg
    import codecs

    abspath = "/bigdata/words"

    # 结巴加载用户词典
    userDict_path = os.path.join(abspath, "ITKeywords.txt")
    jieba.load_userdict(userDict_path)

    # 停用词文本
    stopwords_path = os.path.join(abspath, "stopwords.txt")

    def get_stopwords_list():
        """返回stopwords列表"""
        stopwords_list = [i.strip()
                          for i in codecs.open(stopwords_path).readlines()]
        return stopwords_list

    # 所有的停用词列表
    stopwords_list = get_stopwords_list()

    class TextRank(jieba.analyse.TextRank):
        def __init__(self, window=20, word_min_len=2):
            super(TextRank, self).__init__()
            self.span = window  # 窗口大小
            self.word_min_len = word_min_len  # 单词的最小长度
            # 要保留的词性，根据jieba github ，具体参见https://github.com/baidu/lac
            self.pos_filt = frozenset(
                ('n', 'x', 'eng', 'f', 's', 't', 'nr', 'ns', 'nt', "nw", "nz", "PER", "LOC", "ORG"))

        def pairfilter(self, wp):
            """过滤条件，返回True或者False"""

            if wp.flag == "eng":
                if len(wp.word) <= 2:
                    return False

            if wp.flag in self.pos_filt and len(wp.word.strip()) >= self.word_min_len \
                    and wp.word.lower() not in stopwords_list:
                return True

    # TextRank过滤窗口大小为5，单词最小为2
    textrank_model = TextRank(window=5, word_min_len=2)
    allowPOS = ('n', "x", 'eng', 'nr', 'ns', 'nt', "nw", "nz", "c")

    for row in partition:
        tags = textrank_model.textrank(row.sentence, topK=20, withWeight=True, allowPOS=allowPOS, withFlag=False)
        for tag in tags:
            yield row.post_id, row.category_id, tag[0], tag[1]


class UpdateArticle(SparkSessionBase):
    """
    更新文章画像
    """
    SPARK_APP_NAME = "updateArticle"
    ENABLE_HIVE_SUPPORT = True
    ALL_POSTS = False

    def __init__(self):
        self.spark = self._create_spark_session()

        self.cv_path = "hdfs://Hadoop02/headlines/models/CV.model"
        self.idf_path = "hdfs://Hadoop02/headlines/models/IDF.model"

    def get_cv_model(self):
        # 词语与词频统计
        from pyspark.ml.feature import CountVectorizerModel
        cv_model = CountVectorizerModel.load(self.cv_path)
        return cv_model

    def get_idf_model(self):
        from pyspark.ml.feature import IDFModel
        idf_model = IDFModel.load(self.idf_path)
        return idf_model

    @staticmethod
    def compute_keywords_tfidf_topk(words_df, cv_model, idf_model):
        """保存tfidf值高的20个关键词
        :param spark:
        :param words_df:
        :return:
        """
        cv_result = cv_model.transform(words_df)
        tfidf_result = idf_model.transform(cv_result)

        # print("transform compelete")

        # 取TOP-N的TFIDF值高的结果
        def func(partition):
            TOPK = 20
            for row in partition:
                _ = list(zip(row.idfFeatures.indices, row.idfFeatures.values))
                _ = sorted(_, key=lambda x: x[1], reverse=True)
                result = _[:TOPK]
                #         words_index = [int(i[0]) for i in result]
                #         yield row.post_id, row.category_id, words_index

                for word_index, tfidf in result:
                    yield row.post_id, row.category_id, int(word_index), round(float(tfidf), 4)

        _keywordsByTFIDF = tfidf_result.rdd.mapPartitions(func).toDF(["post_id", "category_id", "index", "tfidf"])

        logger.info("INFO 合并文章成功！")

        return _keywordsByTFIDF

    def merge_article_data(self):
        """
        合并业务中增量更新的文章数据
        :return:
        """
        # 获取文章相关数据, 指定过去一个小时整点到整点的更新数据
        # 如：26日：1：00~2：00，2：00~3：00，左闭右开
        self.spark.sql("use pink;")
        ta = self.spark.sql("show tables;")
        _yester = datetime.today().replace(minute=0, second=0, microsecond=0)
        start = datetime.strftime(_yester + timedelta(days=0, hours=-1, minutes=0), "%Y-%m-%d %H:%M:%S")
        end = datetime.strftime(_yester, "%Y-%m-%d %H:%M:%S")

        # 合并后保留：post_id、category_id、category_name、title、content
        # +----------+----------+--------------------+--------------------+
        # | post_id | category_id | title | content |
        # +----------+----------+--------------------+--------------------+
        # | 141462 | 3 | test - 20190316 - 115123 | 今天天气不错，心情很美丽！！！ |
        if self.ALL_POSTS:
            basic_content = self.spark.sql(
                "select post_id,title,content,category_id from posts where id >= 50000 and id < 55000")
        else:
            basic_content = self.spark.sql(
                "select post_id,title,content,category_id from posts where update_time >= '{}' and update_time <= '{}'".format(
                    start, end))
        # 增加category的名字，后面会使用
        basic_content.registerTempTable("temparticle")
        category_basic_content = self.spark.sql(
            "select t.*, n.category_name from temparticle t left join categories n on t.category_id=n.category_id")

        # 利用concat_ws方法，将多列数据合并为一个长文本内容（频道，标题以及内容合并）
        self.spark.sql("use article;")
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
        logger.info("INFO 增量更新的文章数据成功！")
        return sentence_df

    def generate_article_label(self, sentence_df):
        """
        生成文章标签  tfidf, textrank
        :param sentence_df: 增量的文章内容
        :return:
        """
        # 进行分词
        words_df = sentence_df.rdd.mapPartitions(segmentation).toDF(["post_id", "category_id", "words"])

        cv_model = self.get_cv_model()
        idf_model = self.get_idf_model()

        keywords_list_with_idf = list(zip(cv_model.vocabulary, idf_model.idf.toArray()))

        def func(data):
            for index in range(len(data)):
                data[index] = list(data[index])
                data[index].append(index)
                data[index][1] = float(data[index][1])

        func(keywords_list_with_idf)
        sc = self.spark.sparkContext
        rdd = sc.parallelize(keywords_list_with_idf, numSlices=200)
        df = rdd.toDF(["keywords", "idf", "index"])
        df.write.insertInto('idf_keywords_values')

        # 1、保存所有的词的idf的值，利用idf中的词的标签索引
        # 工具与业务隔离
        _keywordsByTFIDF = UpdateArticle.compute_keywords_tfidf_topk(words_df, cv_model, idf_model)

        keywordsIndex = self.spark.sql("select keyword, index idf from idf_keywords_values")
        keywordsByTFIDF = _keywordsByTFIDF.join(keywordsIndex, keywordsIndex.idf == _keywordsByTFIDF.index).select(
            ["post_id", "category_id", "keyword", "tfidf"])

        keywordsByTFIDF.write.insertInto("tfidf_keywords_values")

        del cv_model
        del idf_model
        del words_df
        del _keywordsByTFIDF
        del keywords_list_with_idf
        gc.collect()

        # 计算textrank
        textrank_keywords_df = sentence_df.rdd.mapPartitions(textrank).toDF(
            ["post_id", "category_id", "keyword", "textrank"])
        textrank_keywords_df.write.insertInto("textrank_keywords_values")

        return textrank_keywords_df, keywordsIndex

    def get_article_profile(self, textrank, keywordsIndex):
        """
        文章画像主题词建立
        :param idf: 所有词的idf值
        :param textrank: 每个文章的textrank值
        :return: 返回建立号增量文章画像
        """
        keywordsIndex = keywordsIndex.withColumnRenamed("keyword", "keyword1")
        result = textrank.join(keywordsIndex, textrank.keyword == keywordsIndex.keyword1)
        # 1、关键词（词，权重）
        _articleKeywordsWeights = result.withColumn("weights", result.textrank * result.idf).select(
            ["post_id", "category_id", "keyword", "weights"])

        # 合并关键词权重到字典
        _articleKeywordsWeights.registerTempTable("temptable")
        articleKeywordsWeights = self.spark.sql(
            "select post_id, min(category_id) category_id, collect_list(keyword) keyword_list, collect_list(weights) weights_list from temptable group by post_id")

        def _func(row):
            return row.post_id, row.category_id, dict(zip(row.keyword_list, row.weights_list))

        articleKeywords = articleKeywordsWeights.rdd.map(_func).toDF(["post_id", "category_id", "keywords"])

        # 2、主题词
        # 将tfidf和textrank共现的词作为主题词
        topic_sql = """
                select t.post_id post_id2, collect_set(t.keyword) topics from tfidf_keywords_values t
                inner join 
                textrank_keywords_values r
                where t.keyword=r.keyword
                group by post_id2
                """
        articleTopics = self.spark.sql(topic_sql)
        # 3、将主题词表和关键词表进行合并，插入表
        articleProfile = articleKeywords.join(articleTopics,
                                              articleKeywords.post_id == articleTopics.post_id2).select(
            ["post_id", "category_id", "keywords", "topics"])
        articleProfile.write.insertInto("article_profile")

        # 删除暂存数据
        self.spark.sql("truncate table idf_keywords_values;")

        del keywordsIndex
        del _articleKeywordsWeights
        del articleKeywords
        del articleTopics
        gc.collect()

        logger.info("INFO 文章画像主题词建立成功！")

        return articleProfile

    def compute_article_similar(self, articleProfile):
        """
        计算增量文章与历史文章的相似度 word2vec
        :return:
        """

        # 得到要更新的新文章通道类别(不采用)
        # all_category = set(articleProfile.rdd.map(lambda x: x.category_id).collect())
        def avg(row):
            x = 0
            for v in row.vectors:
                x += v
            #  将平均向量作为article的向量
            return row.post_id, row.category_id, x / len(row.vectors)

        for category_id, category_name in categoryInfo.items():

            from pyspark.ml.feature import Word2VecModel

            profile = articleProfile.filter('category_id = {}'.format(category_id))
            wv_model = Word2VecModel.load(
                "hdfs://Hadoop02/headlines/models/%s.word2vec" % (category_id))
            vectors = wv_model.getVectors()

            # 计算向量
            profile.registerTempTable("incremental")
            articleKeywordsWeights = ua.spark.sql(
                "select post_id, category_id, keyword, weight from incremental LATERAL VIEW explode(keywords) AS keyword, weight where category_id=%d" % category_id)

            articleKeywordsWeightsAndVectors = articleKeywordsWeights.join(vectors,
                                                                           vectors.word == articleKeywordsWeights.keyword,
                                                                           "inner")
            articleKeywordVectors = articleKeywordsWeightsAndVectors.rdd.map(
                lambda r: (r.post_id, r.category_id, r.keyword, float(r.weight) * r.vector)).toDF(
                ["post_id", "category_id", "keyword", "weightingVector"])

            articleKeywordVectors.registerTempTable("tempTable")
            articleVector = self.spark.sql(
                "select post_id, min(category_id) category_id, collect_set(weightingVector) vectors from tempTable group by post_id").rdd.map(
                avg).toDF(["post_id", "category_id", "articleVector"])

            # 写入数据库
            def toArray(row):
                return row.post_id, row.category_id, [float(i) for i in row.articleVector.toArray()]

            articleVector = articleVector.rdd.map(toArray).toDF(['post_id', 'category_id', 'articleVector'])
            articleVector.write.insertInto("article_vector")

            import gc
            del wv_model
            del vectors
            del articleKeywordsWeights
            del articleKeywordsWeightsAndVectors
            del articleKeywordVectors
            gc.collect()

            # 得到历史数据, 转换成固定格式使用LSH进行求相似
            train = self.spark.sql("select * from article_vector where category_id=%d" % category_id)

            def _array_to_vector(row):
                return row.post_id, Vectors.dense(row.articleVector)

            train = train.rdd.map(_array_to_vector).toDF(['post_id', 'articleVector'])
            test = articleVector.rdd.map(_array_to_vector).toDF(['post_id', 'articleVector'])

            brp = BucketedRandomProjectionLSH(inputCol='articleVector', outputCol='hashes', seed=12345,
                                              bucketLength=1.0)
            model = brp.fit(train)
            similar = model.approxSimilarityJoin(test, train, 2.0, distCol='EuclideanDistance')

            def save_hbase(partition):
                import happybase
                for row in partition:
                    pool = happybase.ConnectionPool(size=10, host='hadoop102',
                                                    autoconnect=True, transport='framed', protocol='compact')
                    # article_similar post_id similar:post_id sim
                    with pool.connection() as conn:
                        table = conn.table("article_similar")
                        for row in partition:
                            if row.datasetA.post_id == row.datasetB.post_id:
                                pass
                            else:
                                table.put(str(row.datasetA.post_id).encode(),
                                          {b"similar:%d" % row.datasetB.post_id: b"%0.4f" % row.EuclideanDistance})
                        conn.close()

            similar.foreachPartition(save_hbase)


if __name__ == '__main__':
    ua = UpdateArticle()
    sentence_df = ua.merge_article_data()
    if sentence_df.rdd.collect():
        rank, idf = ua.generate_article_label(sentence_df)
        articleProfile = ua.get_article_profile(rank, idf)
        ua.compute_article_similar(articleProfile)
