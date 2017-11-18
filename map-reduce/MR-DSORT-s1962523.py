import MapReduce

# The main challenge is to partition the data in the map.
#
# The naive approach is to send all data into a single
# Reducer like 'mr.emit_intermeidate(0, integer)'. However,
# this does not take advantage of the parallelism that
# MapReduce gives us.
#
# Another naive approach is to let the Shuffler take
# care of the sorting by emitting each integer as key:
# mr.emit_intermeidate(integer, 0). This is not very 
# efficient either.
#
# In order to get nice complexity numbers and to make my
# job easier, I would like to make two assumptions ;)
# I know that the input consists of integers between 
# one and 1 million. First assumption is that these
# integers were drawn from a uniform distribtion.
# Second assumption is that we have access to 20 chuck
# servers (integers.json contains 20 lines). These
# give me a nice property: approx. same-sized buckets:

REDUCER_SIZE = 20
BUCKET_SIZE = 10**6 / REDUCER_SIZE

def mapper(key, value):
    for item in value:
        bucket = item // BUCKET_SIZE
        mr.emit_intermediate(bucket, item)

def reducer(bucket, list_of_values):
    sorted_list = sorted(list_of_values)
    print("Bucket {} has {} items".format(int(bucket), len(sorted_list)))
    mr.emit(sorted_list)

# ____________________________________________________________________________
# This code remains unmodified in all programs, except for the input file name.

if __name__ == '__main__':
    data = open("integers.json")
    mr = MapReduce.MapReduce()
    mr.execute(data, mapper, reducer)
