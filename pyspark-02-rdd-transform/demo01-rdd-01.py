# -*- coding:utf-8 -*-
# author:zyj

from pyspark import SparkConf,SparkContext

conf=SparkConf().setAppName('demo5').setMaster("local[1]")
sc = SparkContext(conf=conf)

data = [1,2,3,4,5,6,7,8,9]

rdd1 = sc.parallelize(data, 3)

# 一对一
rdd2 = rdd1.map(lambda x : x + 5)

# 过滤
rdd3 = rdd1.filter(lambda x : x > 5)

def f4(x):
    return [i for i in range(x, 20)]

# 一对多
rdd4 = rdd3.flatMap(f4)

# 抽样（是否放回， 比例， 随机种子）
rdd5 = rdd4.sample(False, 0.5, 1212)

# 合并, 将 rdd3 追加到 rdd5 后面
rdd6 = rdd5.union(rdd3)

# 取交集
rdd7 = rdd1.intersection(rdd2)

# 去重, 去除重复的数据，只显示一次
rdd8 = rdd1.union(rdd2).distinct()

print(1,rdd1.collect())
print(2,rdd2.collect())
print(3,rdd3.collect())
print(4,rdd4.collect())
print(5,rdd5.collect())
print(6,rdd6.collect())
print(7,rdd7.collect())
print(8,rdd8.collect())






