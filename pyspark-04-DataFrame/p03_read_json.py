# -*- coding:utf-8 -*-
# author：zyj

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('my_first_app').getOrCreate()

file = r"../datas/03-DataFrame-01.json"
df = spark.read.json(file)
print(df.show())






