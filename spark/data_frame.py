"""
Reading in a DataFrame
"""

from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext("local", "DataFrame")
sc.setLogLevel("ERROR")

sqlContext = SQLContext(sc)


path = "/data/doina/twitterNL/201612/20161231-23.out.gz"

df = sqlContext.read.json(path)
#df.printSchema()

tweets = df.select('id', 'text')
#tweets.printSchema()

tweets.take(5)
