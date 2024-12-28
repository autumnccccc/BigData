import string
import collections
import time
import threading
from concurrent.futures import ThreadPoolExecutor

def load_stopwords(stopwords_file):
    with open(stopwords_file, 'r', encoding="utf-8") as f:
        stopwords = set(line.strip().lower() for line in f.readlines())
    return stopwords

def preprocess_text(text, stopwords):
    text = text.translate(str.maketrans('', '', string.punctuation))  # 去标点符号
    text = text.lower()  # 转小写
    words = text.split()  # 分词
    words = [word for word in words if word not in stopwords and not word.isdigit()]
    return words

def mapper(text, stopwords):
    # print(f"Thread {threading.current_thread().name} is processing a line.")
    words = preprocess_text(text, stopwords)
    local_wc = collections.defaultdict(int)
    for word in words:
        local_wc[word] += 1
    return local_wc

def reducer(word_counts_list):
    final_word_count = collections.defaultdict(int)
    for wc in word_counts_list:
        for word, count in wc.items():
            final_word_count[word] += count
    return final_word_count

def map_reduce(text_file, stopwords_file):
    stopwords = load_stopwords(stopwords_file)
    word_counts_list = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        with open(text_file, 'r', encoding='utf-8') as f:
            tasks = [executor.submit(mapper, line, stopwords) for line in f.readlines()]
        
        # Collect results from all threads
        for task in tasks:
            word_counts_list.append(task.result())

    # 合并结果
    word_count = reducer(word_counts_list)

    # 按照词频排序
    sorted_word_count = sorted(word_count.items(), key=lambda x: x[1], reverse=True)
    return sorted_word_count

if __name__ == '__main__':
    start_time = time.time()
    text_file = 'desc/题目2数据/text.txt'  
    stopwords_file = 'desc/题目2数据/stopword.txt'  

    result = map_reduce(text_file, stopwords_file)
    
    # 输出前200个最常用的词
    with open("./hotword_res.txt", "w", encoding="utf-8") as fp:
        for word, count in result[:200]:
            fp.write(f"{word}: {count}\n")

    end_time = time.time()
    print(f"处理完成，耗时{end_time - start_time:.2f}秒")
