import MapReduce

"""
Author: Omar Ali Sheikh-Omar (s1962523)
"""

from pyspark import SparkContext

sc = SparkContext("local", "Twitter user activity")
sc.setLogLevel("ERROR")

