# -*- coding:utf-8 -*-
# author：zyj

from pyspark.sql import SparkSession

#数据类型
from pyspark.sql.types import StructType, StructField, LongType, StringType

spark=SparkSession.builder.appName('my_first_app').getOrCreate()

# 生成以逗号分隔的数据
stringCSVRDD = spark.sparkContext.parallelize([
    (123, "Katie", 19, "brown"),
    (234, "Michael", 22, "green"),
    (345, "Simone", 23, "blue")])

# 设定数据类型
schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("age", LongType(), True),
    StructField("eyeColor", StringType(), True)
])

# 对RDD应用该模式并且创建DataFrame
swimmers = spark.createDataFrame(stringCSVRDD,schema)

# 利用DataFrame创建一个临时视图
swimmers.registerTempTable("swimmers")

swimmers.show()
