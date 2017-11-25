"""
Author: Omar Ali Sheikh-Omar (s1962523)
"""

from pyspark import SparkContext
import re

FILE_SEPARATOR = ':'
number_of_files = 14

sc = SparkContext("local", "Inverted Index")
sc.setLogLevel("ERROR")

"""
Creates a list of tuples in the form (word, file_name)
"""
def tokenise(input_tuple):
    file_path = input_tuple[0]
    file_content = input_tuple[1]
    sanitised_text = re.sub('[,\.\[\]]', '', file_content.lower())
    words = sanitised_text.split()
    file_name = file_path[file_path.rfind('/')+1:]
    return map(lambda word: (word, file_name), words)

path = '/data/doina/Gutenberg-EBooks'
rdd = sc.wholeTextFiles(path)
rdd = rdd.flatMap(tokenise)
rdd = rdd.reduceByKey(lambda names, file_name: names + FILE_SEPARATOR + file_name if names.find(file_name) == -1 else names)
rdd = rdd.filter(lambda tup: tup[1].count(FILE_SEPARATOR) == number_of_files - 1)
rdd = rdd.map(lambda tup: tup[0])
arr = sorted(rdd.collect())
print(' '.join(arr))
