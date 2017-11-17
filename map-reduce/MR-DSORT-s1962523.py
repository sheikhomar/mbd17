import MapReduce

# The main challenge is to partition the data

# First approach is to send all data into a 
# single reducer. This does not take advantage 
# of the parallelism that MapReduce gives us
def mapper(key, value): 
    for item in value:
        mr.emit_intermediate(0, item)

def reducer(key, list_of_values):
    mr.emit(sorted(list_of_values))

# Second approach let the Shuffler take
# care of the sorting.
def mapper2(key, value):
    for item in value:
        mr.emit_intermediate(item, 'n/a')

def reducer2(key, list_of_values):
    mr.emit(key)

# ____________________________________________________________________________
# This code remains unmodified in all programs, except for the input file name.

if __name__ == '__main__':
    data = open("integers.json")
    mr = MapReduce.MapReduce()
    mr.execute(data, mapper, reducer)
    #mr.execute(data, mapper2, reducer2)
