# -*- coding:utf-8 -*-
# author:zyj

from pyspark import SparkConf,SparkContext

conf=SparkConf().setAppName('demo5').setMaster("local[1]")
sc = SparkContext(conf=conf)

data = [(1,3), (1,7), (2,8), (2,5)]

rdd1 = sc.parallelize(data)

# 数据分组操作
rdd2 = rdd1.groupByKey()

# 数据分组聚合操作
rdd3 = rdd1.reduceByKey(lambda x,y : x+y)

# 排序
rdd4 = rdd1.sortByKey()

# join
rdd5 = rdd1.join(rdd1)

rdd6 = rdd1.cogroup(rdd1)

rdd7 = rdd1.cartesian(rdd1)

print(1, rdd1.collect())
print(2, rdd2.collect())
print(3, rdd3.collect())
print(4, rdd4.collect())
print(5, rdd5.collect())
print(6, rdd6.collect())
print(7, rdd7.collect())









