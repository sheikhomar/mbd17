import MapReduce

"""
Author: Omar Ali Sheikh-Omar (s1962523)
"""

from pyspark import SparkContext

sc = SparkContext("local", "Amazon music sales-rank")
sc.setLogLevel("ERROR")
