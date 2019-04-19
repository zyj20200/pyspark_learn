# -*- coding:utf-8 -*-

from pyspark import SparkConf  # spark配置
from pyspark import SparkContext

# import os
# os.environ['PYSPARK_PYTHON']='/usr/bin/python3.5'

# setAppName 设置application名称，setMaster：local本地运行
conf=SparkConf().setAppName("hello").setMaster("local[1]")
#local[1] 计算机并行运行线程数，*代表用所有可用的线程，默认为1

# spark 上下文环境
sc = SparkContext(conf=conf)

# parallelize 并行化数据，成为RDD
data = ['i love china', 'i love shanghai', 'i love nanjin']
mydata = sc.parallelize(data)

res1 = mydata.map(lambda sen:sen.split()).collect()
res2 = mydata.flatMap(lambda sen:sen.split()).collect()

print(res1)
print(res2)
print('=='*10)

words = mydata.flatMap(lambda sen:sen.split())
pairs = words.map(lambda word:(word,1))
res3 = pairs.reduceByKey(lambda a,b:a+b).collect()
print(res3)

