# -*- coding:utf-8 -*-
# author：zyj

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('my_first_app').getOrCreate()


# 先创建csv文件
import pandas as pd
import numpy as np

# 创建数据，并保存至csv文件中
df=pd.DataFrame(np.random.rand(5,5),columns=['a','b','c','d','e']).applymap(lambda x: int(x*10))
file=r"../datas/p04_test.csv"
df.to_csv(file,index=False)

# 再读取csv文件
monthlySales = spark.read.csv(file, header=True, inferSchema=True)
monthlySales.show()
