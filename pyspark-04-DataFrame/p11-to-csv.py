# -*- coding:utf-8 -*-
# author：zyj

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('my_first_app').getOrCreate()

df = pd.DataFrame(np.random.random((4, 4)),columns=['a', 'b', 'c', 'd'])
spark_df = spark.createDataFrame(df)

# 写到csv
file=r"../datas/p11-test.csv"
spark_df.write.csv(path=file, header=True, sep=",", mode='overwrite')




