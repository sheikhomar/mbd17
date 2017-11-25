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

df = sqlc.read.json('/data/doina/UCSD-Amazon-Data/meta_Digital_Music.json.gz')
rdd = df.select('salesRank.Music').rdd
rdd = rdd.filter(lambda row: not row.Music is None)
rdd = rdd.map(lambda row: (row.Music // N, 1))
rdd = rdd.reduceByKey(lambda acc, i: acc + i)
rdd = rdd.sortBy(lambda tup: tup[0])
print('Bucket size: {}'.format(N))
#print(rdd.take(1200))
rdd = rdd.map(lambda tup: tup[1])
print(rdd.take(100))


"""
How to plot the data:
 1. Uncomment line 23.
 2. Copy the output as data into a local Python script file:

import matplotlib.pyplot as plt
data = [(0, 67), (1, 71), (2, 69), (3, 59), (4, 77), (5, 57), (6, 70), (7, 49), (8, 50), (9, 52) ...
fig, ax = plt.subplots()
X, Y = zip(*data)
ax.bar(X, Y)
plt.show()
"""
