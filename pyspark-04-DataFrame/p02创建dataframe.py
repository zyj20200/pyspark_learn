# -*- coding:utf-8 -*-
# author：zyj

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('my_first_app').getOrCreate()

# 使用自动类型推断的方式创建dataframe
data = [(123, "Katie", 19, "brown"),
        (234, "Michael", 22, "green"),
        (345, "Simone", 23, "blue")]
df = spark.createDataFrame(data, schema=['id', 'name', 'age', 'eyccolor'])

print(df.show())
print(df.count())


















