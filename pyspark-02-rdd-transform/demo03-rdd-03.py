# -*- coding:utf-8 -*-
# author:zyj

from pyspark import SparkConf,SparkContext

conf=SparkConf().setAppName('demo5').setMaster("local[1]")
sc = SparkContext(conf=conf)

data = [('a',3),('a',2),('a',5),('b',3),('b',4),('b',7)]

rdd1 = sc.parallelize(data, 2)

rdd2 = rdd1.aggregateByKey(0, lambda x,y:max(x,y), lambda x,y:x+y)

rdd3 = rdd1.combineByKey(lambda x:[(x, x**2)], lambda x,y:x+[(y, y**2)], lambda x,y:x+y)

print(1, rdd1.collect())
print(2, rdd2.collect())
print(3, rdd3.collect())







