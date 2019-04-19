# -*- coding:utf-8 -*-
# author：zyj

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('my_first_app').getOrCreate()

df = pd.DataFrame(np.random.random((4, 4)),columns=['a', 'b', 'c', 'd'])
spark_df = spark.createDataFrame(df)

# 写到parquet
file=r"../datas/test.parquet"
spark_df.write.parquet(path=file,mode='overwrite')









