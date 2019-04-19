# -*- coding:utf-8 -*-

from pyspark import SparkConf,SparkContext

conf = SparkConf().setAppName('test').setMaster('local')
sc = SparkContext(conf=conf)

list = [1, 2, 3, 4, 5]

rdd = sc.parallelize(list)
rdd.persist()#持久话, 可以存在硬盘,内存等,默认为在内存中存1份,并且序列化

res1 = rdd.filter(lambda x : x % 2==1).count()
res2 = rdd.filter(lambda x : x % 2==0).count()
# filter的结果为true和false, res1是奇数的个数,res2是偶数的个数

print(res1)
print(res2)
