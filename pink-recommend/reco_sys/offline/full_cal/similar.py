import os
import sys

# 3.---------article_vector,hbase:article_similar
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

from setting.default import categoryInfo
from pyspark.ml.feature import Word2Vec


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


class TrainWord2VecModel(SparkSessionBase):
    SPARK_APP_NAME = "Word2Vec"

    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()


w2v = TrainWord2VecModel()
for k, v in categoryInfo.items():
    # 这里训练一个频道模型演示即可
    print("分类页面" + v)
    w2v.spark.sql("use article;")
    article = w2v.spark.sql("select * from article_data where category_id='{}';".format(k))
    if article.rdd.isEmpty():
        continue
    words_df = article.rdd.mapPartitions(segmentation).toDF(['post_id', 'category_id', 'words'])
    print("words_df开始")
    new_word2Vec = Word2Vec(vectorSize=100, inputCol="words", outputCol="model", minCount=3)
    new_model = new_word2Vec.fit(words_df)
    new_model.save("hdfs://hadoop102/headlines/models/" + v + ".word2vec")
    print("words_df=结束")

    from pyspark.ml.feature import Word2VecModel

    wv_model = Word2VecModel.load(
        "hdfs://hadoop102/headlines/models/%s.word2vec" % (v))
    vectors = wv_model.getVectors()

    # 选出新增的文章的画像做测试，上节计算的画像中有不同频道的，我们选取Python频道的进行计算测试
    # 新增的文章画像获取部分
    print("文章画像开始")
    profile = w2v.spark.sql("select * from article_profile where category_id='{}';".format(k))
    # profile = articleProfile.filter('category_id = {}'.format(category_id))

    profile.registerTempTable("incremental")
    articleKeywordsWeights = w2v.spark.sql(
        "select post_id, category_id, keyword, weight from incremental LATERAL VIEW explode(keywords) AS keyword, weight")
    _article_profile = articleKeywordsWeights.join(vectors, vectors.word == articleKeywordsWeights.keyword, "inner")
    _article_profile.show()
    if _article_profile.rdd.isEmpty():
        continue
    articleKeywordVectors = _article_profile.rdd.map(
        lambda row: (row.post_id, row.category_id, row.keyword, float(row.weight) * row.vector)).toDF(
        ["post_id", "category_id", "keyword", "weightingVector"])
    print("文章画像结束")


    def avg(row):
        x = 0
        for v in row.vectors:
            x += v
        #  将平均向量作为article的向量
        return row.post_id, row.category_id, x / len(row.vectors)


    articleKeywordVectors.registerTempTable("tempTable")
    articleVector = w2v.spark.sql(
        "select post_id, min(category_id) category_id, collect_set(weightingVector) vectors from tempTable group by post_id").rdd.map(
        avg).toDF(["post_id", "category_id", "articleVector"])


    def toArray(row):
        return row.post_id, row.category_id, [float(i) for i in row.articleVector.toArray()]


    print("articleVector开始")
    articleVector = articleVector.rdd.map(toArray).toDF(['post_id', 'category_id', 'articleVector'])

    articleVector.write.insertInto("article_vector")
    print("articleVector结束")

    from pyspark.ml.linalg import Vectors


    def _array_to_vector(row):
        return row.post_id, Vectors.dense(row.articleVector)


    print("train开始")
    train = articleVector.rdd.map(_array_to_vector).toDF(['post_id', 'articleVector'])

    from pyspark.ml.feature import BucketedRandomProjectionLSH

    brp = BucketedRandomProjectionLSH(inputCol='articleVector', outputCol='hashes', numHashTables=4.0,
                                      bucketLength=10.0)
    model = brp.fit(train)

    similar = model.approxSimilarityJoin(train, train, 2.0, distCol='EuclideanDistance')
    print("train结束")

    print("hbase开始")


    def save_hbase(partition):
        import happybase
        pool = happybase.ConnectionPool(size=10, host='hadoop102',
                                        autoconnect=True, transport='framed', protocol='compact')
        with pool.connection() as conn:
            # 建议表的连接
            table = conn.table('article_similar')
            for row in partition:
                if row.datasetA.post_id == row.datasetB.post_id:
                    pass
                else:
                    table.put(str(row.datasetA.post_id).encode(),
                              {"similar:{}".format(row.datasetB.post_id).encode(): b'%0.4f' % (
                                  row.EuclideanDistance)})
            # 手动关闭所有的连接
            conn.close()


    similar.foreachPartition(save_hbase)
    print("hbase结束")
