import MapReduce

"""
Author: Omar Ali Sheikh-Omar (s1962523)
"""

def mapper(key, value): 
    mr.emit_intermediate('key', max(value))

def reducer(key, list_of_values):
    mr.emit(max(list_of_values))

# ____________________________________________________________________________
# This code remains unmodified in all programs, except for the input file name.

if __name__ == '__main__':
    data = open("integers.json")
    mr = MapReduce.MapReduce()
    mr.execute(data, mapper, reducer)
