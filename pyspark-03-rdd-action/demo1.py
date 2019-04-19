# -*- coding:utf-8 -*-
# author:zyj

from pyspark import SparkConf,SparkContext

conf=SparkConf().setAppName('demo5').setMaster("local[1]")
sc = SparkContext(conf=conf)


