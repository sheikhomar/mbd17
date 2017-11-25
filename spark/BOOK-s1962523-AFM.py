"""
Author: Omar Ali Sheikh-Omar (s1962523)
"""

from pyspark import SparkContext
from pyspark.sql import SQLContext
import re

sc = SparkContext("local", "Hashtags")
sc.setLogLevel("ERROR")

N = 1000

sqlc = SQLContext(sc)

df = sqlc.read.json('/data/doina/UCSD-Amazon-Data/meta_Books_sample.json.gz')
rdd = df.select('related.also_bought').rdd
rdd = rdd.filter(lambda row: not row.also_bought is None)
rdd = rdd.flatMap(lambda row: map(lambda s: (s, 1), set(row.also_bought)))
rdd = rdd.reduceByKey(lambda acc, i: acc + i)
rdd = rdd.sortBy(lambda tup: tup[1], ascending=False)

top1 = rdd.take(1)

top1_asin = top1[0][0]
top1_count = top1[0][1]
top1_record = df.filter('asin = "{}"'.format(top1_asin)).first()

print('ASIN:   {}'.format(top1_asin))
print('Count:  {}'.format(top1_count))
print('Record:\n{}'.format(top1_record))
