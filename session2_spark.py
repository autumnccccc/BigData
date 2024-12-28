import string
import collections
import time
from pyspark import SparkContext, SparkConf

def load_stopwords(stopwords_file):
    """加载停用词文件"""
    with open(stopwords_file, 'r', encoding="utf-8") as f:
        stopwords = set(line.strip().lower() for line in f.readlines())
    return stopwords

def preprocess_text(text, stopwords):
    """文本预处理：去标点，转小写，分词"""
    text = text.translate(str.maketrans('', '', string.punctuation))  # 去标点符号
    text = text.lower()  # 转小写
    words = text.split()  # 分词
    words = [word for word in words if word not in stopwords and not word.isdigit()]
    return words

def mapper(line, stopwords):
    """每行文本的映射函数：分词并计算词频"""
    words = preprocess_text(line, stopwords)
    return [(word, 1) for word in words]

def reducer(accum, new):
    """聚合函数：合并词频"""
    return accum + new

def map_reduce(text_file, stopwords_file):
    # 初始化 SparkContext
    conf = SparkConf().setAppName("WordCountApp")
    sc = SparkContext(conf=conf)

    stopwords = load_stopwords(stopwords_file)
    text_rdd = sc.textFile(text_file)
    mapped_rdd = text_rdd.flatMap(lambda line: mapper(line, stopwords))
    
    word_counts_rdd = mapped_rdd.reduceByKey(lambda x, y: x + y)
    sorted_word_count = word_counts_rdd.sortBy(lambda x: x[1], ascending=False)
    result = sorted_word_count.collect()

    sc.stop()  # 停止 SparkContext
    
    return result

if __name__ == '__main__':
    start_time = time.time()
    text_file = 'desc/题目2数据/text.txt'  
    stopwords_file = 'desc/题目2数据/stopword.txt'  

    result = map_reduce(text_file, stopwords_file)
    
    # 输出前200个最常用的词
    with open("./hotword_res_spark.txt", "w", encoding="utf-8") as fp:
        for word, count in result[:200]:
            fp.write(f"{word}: {count}\n")

    end_time = time.time()
    print(f"处理完成，耗时{end_time - start_time:.2f}秒")
