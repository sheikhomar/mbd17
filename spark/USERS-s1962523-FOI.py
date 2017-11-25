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
rdd = df.select('user.screen_name').rdd
rdd = rdd.map(lambda row: (row.screen_name, 1))
rdd = rdd.reduceByKey(lambda acc, i: acc + i)
rdd = rdd.sortBy(lambda tup: tup[1], ascending=False)
print(rdd.take(40))
