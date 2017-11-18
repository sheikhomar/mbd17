import MapReduce
import re

"""
Author: Omar Ali Sheikh-Omar (s1962523)
"""

def mapper(key, value): 
    matches = re.findall(r'#\w+', value["text"], flags=0)

    # Build hashtags dictionary
    hashtag_dict = {}
    for m in matches:
        if m not in hashtag_dict:
            hashtag_dict[m] = 1
        else:
            hashtag_dict[m] += 1

    # Emit result
    for m in hashtag_dict:
        mr.emit_intermediate(m, hashtag_dict[m])

def reducer(key, list_of_values):
    total_count = sum(list_of_values)
    if total_count >= 20:
        mr.emit((key, total_count))

# ____________________________________________________________________________
# This code remains unmodified in all programs, except for the input file name.

if __name__ == '__main__':
    data = open("one_hour_of_tweets.json")
    mr = MapReduce.MapReduce()
    mr.execute(data, mapper, reducer)
