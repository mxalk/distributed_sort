import sys

Input_File = 'gensortDataFile'
Output_File = 'Output_gensortDataFile_rs'

def insertion_sort(list):
    for i in range(1, len(list)):
        current_value = list[i] 
        position = i   
        while position > 0 and list[position-1][0:4] > current_value[0:4]:
                list[position] = list[position-1]
                position = position - 1
        list[position] = current_value


def radix_sort(list):
    char_list = [chr(x) for x in range(32,127)]
    max_length = 4 ### compare first 4 characters

    for i in range(max_length):
        bucket = {}
        for char in char_list:
            bucket[ord(char)] = []
        for tuple in list:
            temp_word = tuple[0:4]
            bucket[ord(temp_word[max_length - i - 1])].append(tuple)
 
        index = 0
        for char in char_list:
            if (len(bucket[ord(char)])) != 0:
                for i in bucket[ord(char)]:
                    list[index] = i
                    index = index + 1
        

if __name__ == "__main__":
    try:    
        with open(Input_File,'r') as raw_input:
            data = raw_input.read().strip()
            tuple = data.split("\n")
    except FileNotFoundError:
        print('Cannot Open The File!')

    #insertion_sort(tuple) 
    radix_sort(tuple)
    # for i in range(len(tuple)) :
    #     print(ord(tuple[i][0]),end="--")
    #     print(ord(tuple[i][1]))
    
    try:
        with open(Output_File,'w+') as output:
            for i in range(0,len(tuple)):
                output.write(tuple[i]+'\n')
    except FileNotFoundError:
        print('Cannot Open The File!')
    
