import sys

Input_File = 'three'
Output_File = 'Output_gensortDataFile'

def insertion_sort(list):
    for i in range(1, len(list)):
        current_value = list[i] 
        position = i   
        while position > 0 and list[position-1][0:4] > current_value[0:4]:
                list[position] = list[position-1]
                position = position - 1
        list[position] = current_value

if __name__ == "__main__":
    try:    
        with open(Input_File,'r') as raw_input:
            data = raw_input.read().strip()
            tuple = data.split("\n")
    except FileNotFoundError:
        print('Cannot Open The File!')

    insertion_sort(tuple) 

    # for i in range(len(tuple)) :
    #     print(ord(tuple[i][0]),end="--")
    #     print(ord(tuple[i][1]))
    
    try:
        with open(Output_File,'w+') as output:
            for i in range(0,len(tuple)):
                output.write(tuple[i]+'\n')
    except FileNotFoundError:
        print('Cannot Open The File!')
    
