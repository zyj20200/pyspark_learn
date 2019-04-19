# -*- coding:utf-8 -*-

from pyspark import SparkContext,SparkConf

filepath = '../file/1.txt'

conf = SparkConf().setAppName('read_file').setMaster('local')
sc = SparkContext(conf=conf)

# 读取文件
rdd_file = sc.textFile(filepath)

# 切分单词
words = rdd_file.flatMap(lambda line: line.split(' '))

"""
# 单机下
# 统计单词词频，并降序排排列
counts = words.map(lambda word: (word, 1)).reduceByKey(lambda x,y: x+y).collect()
sorted_res = sorted(counts, key=lambda word:word[1], reverse=True)
print(sorted_res)
"""

"""
# 分布式下操作
# 先排序，在collect
"""
counts = words.map(lambda word: (word, 1)).reduceByKey(lambda x,y: x+y)
#sorted_res = counts.sortBy(lambda x:x[1], ascending=False).collect()
sorted_res = counts.map(lambda x:(x[1],x[0])).sortByKey(ascending=False).map(lambda x:(x[1],x[0])).collect()
# sortedByKey默认使用第一个值作为key，通过map颠倒顺序，再排序，再将排序后的结果颠倒
print(sorted_res)