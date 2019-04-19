# -*- coding:utf-8 -*-
# author：zyj

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('my_first_app').getOrCreate()

# 如果不指定schema则用pandas的列名
df = pd.DataFrame(np.random.random((4,4)))
print(df)
print('========================')

spark_df = spark.createDataFrame (df,schema=['a','b','c','d'])
spark_df.show()



