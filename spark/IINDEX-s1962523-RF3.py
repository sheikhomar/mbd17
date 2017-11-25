import MapReduce

"""
Author: Omar Ali Sheikh-Omar (s1962523)
"""

from pyspark import SparkContext

sc = SparkContext("local", "Inverted Index")
sc.setLogLevel("ERROR")

def to_word_file_tuple(filePath, fileContent):
    words = fileContent.lower().split()
    fileName = filePath[filePath.rfind('/')+1:]
    return map(lambda word: (word, fileName), words)

def pretty_print(list_of_tuples):
    for tup in list_of_tuples:
        print(u'Key: {} Value: {}'.format(tup[0], tup[1]))


path = "/data/doina/Gutenberg-EBooks"
rdd = sc.wholeTextFiles(path)
rdd2 = rdd.flatMap(to_word_file_tuple)
pretty_print(rdd2.take(20))
