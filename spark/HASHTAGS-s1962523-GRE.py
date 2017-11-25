"""
Author: Omar Ali Sheikh-Omar (s1962523)
"""

from pyspark import SparkContext
from pyspark.sql import SQLContext
import re

sc = SparkContext("local", "Hashtags")
sc.setLogLevel("ERROR")

def tokenise(row):
    sanitised_text = row.text.lower()
    return re.findall(r'#\w+', sanitised_text, flags=0)

sqlc = SQLContext(sc)
df = sqlc.read.json("/data/doina/twitterNL/201612/20161231-23.out.gz")
rdd = df.select('text').rdd
rdd = rdd.flatMap(tokenise)
rdd = rdd.map(lambda hashtag: (hashtag, 1))
rdd = rdd.reduceByKey(lambda acc, i: acc + i)
rdd = rdd.sortBy(lambda tup: tup[1], ascending=False)
#tweets.printSchema()
print(rdd.take(20))
