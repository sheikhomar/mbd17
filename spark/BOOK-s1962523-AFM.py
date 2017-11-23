import MapReduce

"""
Author: Omar Ali Sheikh-Omar (s1962523)
"""

from pyspark import SparkContext

sc = SparkContext("local", "Amazon also-bought books")
sc.setLogLevel("ERROR")

