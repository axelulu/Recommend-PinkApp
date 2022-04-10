import os
import sys

# 2.---------idf_keywords_values,tfidf_keywords_values,textrank_keywords_values,article_profile
# 如果当前代码文件运行测试需要加入修改路径，避免出现后导包问题
BASE_DIR = os.path.dirname(os.path.dirname(os.getcwd()))
sys.path.insert(0, os.path.join(BASE_DIR))
PYSPARK_PYTHON = "/usr/local/python3/bin/python3"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_181-cloudera"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
from offline import SparkSessionBase


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
                          for i in codecs.open(stopwords_path, 'r', 'utf-8').readlines()]
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


class KeywordsToTfidf(SparkSessionBase):
    SPARK_APP_NAME = "keywordsByTFIDF"
    # SPARK_EXECUTOR_MEMORY = "6g"

    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()


ktt = KeywordsToTfidf()
ktt.spark.sql("use article")
article_dataframe = ktt.spark.sql("select * from article_data;")
words_df = article_dataframe.rdd.mapPartitions(segmentation).toDF(["post_id", "category_id", "words"])
# 词语与词频统计
from pyspark.ml.feature import CountVectorizer

# 总词汇的大小，文本中必须出现的次数
cv = CountVectorizer(inputCol="words", outputCol="countFeatures", vocabSize=200 * 10000, minDF=1.0)
# 训练词频统计模型
cv_model = cv.fit(words_df)
cv_model.write().overwrite().save("hdfs://hadoop102/headlines/models/CV.model")
# 词语与词频统计
from pyspark.ml.feature import CountVectorizerModel

cv_model = CountVectorizerModel.load("hdfs://hadoop102/headlines/models/CV.model")
# 得出词频向量结果
cv_result = cv_model.transform(words_df)
# 训练IDF模型
from pyspark.ml.feature import IDF

idf = IDF(inputCol="countFeatures", outputCol="idfFeatures")
idfModel = idf.fit(cv_result)
idfModel.write().overwrite().save("hdfs://hadoop102/headlines/models/IDF.model")
#
# print(cv_model.vocabulary)
#
# # 每个词的逆文档频率，在历史13万文章当中是固定的值，也作为后面计算TFIDF依据
# print(idfModel.idf.toArray()[:20])

from pyspark.ml.feature import CountVectorizerModel

cv_model = CountVectorizerModel.load("hdfs://hadoop102/headlines/models/CV.model")

from pyspark.ml.feature import IDFModel

idf_model = IDFModel.load("hdfs://hadoop102/headlines/models/IDF.model")
keywords_list_with_idf = list(zip(cv_model.vocabulary, idf_model.idf.toArray()))


def func(data):
    for index in range(len(data)):
        data[index] = list(data[index])
        data[index].append(index)
        data[index][1] = float(data[index][1])


func(keywords_list_with_idf)
sc = ktt.spark.sparkContext
rdd = sc.parallelize(keywords_list_with_idf, numSlices=200)
df = rdd.toDF(["keywords", "idf", "index"])
df.write.insertInto('idf_keywords_values')

cv_result = cv_model.transform(words_df)
tfidf_result = idf_model.transform(cv_result)


def func(partition):
    TOPK = 20
    for row in partition:
        # 找到索引与IDF值并进行排序
        _ = list(zip(row.idfFeatures.indices, row.idfFeatures.values))
        _ = sorted(_, key=lambda x: x[1], reverse=True)
        result = _[:TOPK]
        for word_index, tfidf in result:
            yield row.post_id, row.category_id, int(word_index), round(float(tfidf), 4)


_keywordsByTFIDF = tfidf_result.rdd.mapPartitions(func).toDF(["post_id", "category_id", "index", "tfidf"])

# _keywordsByTFIDF.show()

# 利用结果索引与”idf_keywords_values“合并知道词
keywordsIndex = ktt.spark.sql("select keyword, index idx from idf_keywords_values")
# 利用结果索引与”idf_keywords_values“合并知道词
keywordsByTFIDF = _keywordsByTFIDF.join(keywordsIndex, keywordsIndex.idx == _keywordsByTFIDF.index).select(
    ["post_id", "category_id", "keyword", "tfidf"])
keywordsByTFIDF.write.insertInto("tfidf_keywords_values")


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
                          for i in codecs.open(stopwords_path, 'r', 'utf-8').readlines()]
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


# 计算textrank
textrank_keywords_df = article_dataframe.rdd.mapPartitions(textrank).toDF(
    ["post_id", "category_id", "keyword", "textrank"])

textrank_keywords_df.write.insertInto("textrank_keywords_values")

idf = ktt.spark.sql("select * from idf_keywords_values")
idf = idf.withColumnRenamed("keyword", "keyword1")
result = textrank_keywords_df.join(idf, textrank_keywords_df.keyword == idf.keyword1)
keywords_res = result.withColumn("weights", result.textrank * result.idf).select(
    ["post_id", "category_id", "keyword", "weights"])

keywords_res.registerTempTable("temptable")
merge_keywords = ktt.spark.sql(
    "select post_id, min(category_id) category_id, collect_list(keyword) keywords, collect_list(weights) weights from temptable group by post_id")


# 合并关键词权重合并成字典
def _func(row):
    return row.post_id, row.category_id, dict(zip(row.keywords, row.weights))


keywords_info = merge_keywords.rdd.map(_func).toDF(["post_id", "category_id", "keywords"])

topic_sql = """
                select t.post_id post_id2, collect_set(t.keyword) topics from tfidf_keywords_values t
                inner join 
                textrank_keywords_values r
                where t.keyword=r.keyword
                group by post_id2
                """
article_topics = ktt.spark.sql(topic_sql)
article_topics.show()
article_profile = keywords_info.join(article_topics, keywords_info.post_id == article_topics.post_id2).select(
    ["post_id", "category_id", "keywords", "topics"])

article_profile.show()
article_profile.write.insertInto("article_profile")
print("完成")
