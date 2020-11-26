import json
import pickle
from datetime import datetime

modifiers = ['K', 'M', 'G']

# parses a number 
def parseNumber(string):
    modifier = string[-1]
    number = int(string[:-1])
    if modifier not in modifiers:
        return string
    exponent = 10*(modifiers.index(modifier)+1)
    result = number * 2**exponent
    return result

def getTimeDiff(start_time):
    end_time = datetime.now()
    time_diff = (end_time - start_time)
    return time_diff.total_seconds()

def encodeData(data):
    return json.dumps(data).encode() + b'\0'
    # return pickle.dumps(data)

def decodeData(data):
    return json.loads(data.decode("ascii"))
    # return pickle.loads(data)