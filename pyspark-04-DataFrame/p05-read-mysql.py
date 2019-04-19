# -*- coding:utf-8 -*-
# author：zyj

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('my_first_app').getOrCreate()


# 此时需要将mysql-jar驱动放到spark-2.2.0-bin-hadoop2.7\jars下面
# 单机环境可行，集群环境不行
# 重新执行
df1 = spark.read.format('jdbc').options(
    url='jdbc:mysql://127.0.0.1/sys',
    dbtable='sys_config',
    user='root',
    password='123456'
    ).load()
print(df1.show())

# 也可以传入SQL语句

sql="(select * from sys.sys_config) t"
df2 = spark.read.format('jdbc').options(
    url='jdbc:mysql://127.0.0.1',
    dbtable=sql,
    user='root',
    password='123456'
    ).load()
df2.show()
