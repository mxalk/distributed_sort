
modifiers = ['K', 'M', 'G']
def parseNumber(string):
    modifier = string[-1]
    number = string[:-1]
    if modifier not in modifiers:
        return string
    exponent = 10*(modifiers.index(modifier)+1)
    return number * 2**exponent