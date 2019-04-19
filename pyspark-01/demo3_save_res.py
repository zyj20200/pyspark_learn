# -*- coding:utf-8 -*-

from pyspark import SparkContext,SparkConf

filepath = '../file/1.txt'

conf = SparkConf().setAppName('read_file').setMaster('local[*]')
sc = SparkContext(conf=conf)

# 读取文件
rdd_file = sc.textFile(filepath,minPartitions=4)

# 切分单词
words = rdd_file.flatMap(lambda line: line.split(' '))

# 统计词频
counts = words.map(lambda word: (word, 1)).reduceByKey(lambda x,y: x+y)

# 倒叙排列
sorted_res = counts.map(lambda x:(x[1],x[0])).sortByKey(ascending=False).map(lambda x:(x[1],x[0]))

# 导出结果,一个名为1_sorted的文件夹
sorted_res.saveAsTextFile('../file/1_sorted')