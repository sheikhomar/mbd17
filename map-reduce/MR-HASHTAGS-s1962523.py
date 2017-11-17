import MapReduce
import re

def mapper(key, value): 
    matches = re.findall(r'#\w+', value["text"], flags=0)

    hashtag_dict = {}
    for match in matches:
        hashtag = match
        if hashtag not in hashtag_dict:
            hashtag_dict[hashtag] = 1
        else:
            hashtag_dict[hashtag] += 1
    for hashtag in hashtag_dict:
        mr.emit_intermediate(hashtag, hashtag_dict[hashtag])

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
