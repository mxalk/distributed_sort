import math
import numpy as np

def create_partition_dict(n_partitions):
    # The characters which keys consists of.
    char_space = ": !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
    
    # Create a array of all characters
    char_array = [char for char in char_space]

    # Split the array in n roughly equal size parts
    char_array_split_parts = np.array_split(char_array, n_partitions)

    # Create a dictionary which has as key the characters
    # and as value the partition it belongs to.
    partition_dict = {}
    for i in range(len(char_array_split_parts)):
        partition = char_array_split_parts[i]
        for char in partition:
            partition_dict[char] = int(i)
    
    return partition_dict

# Given an entry and a partition dictionary, returns the partition of the entry.
def choose_partition(partition_dict, entry_key):
    return partition_dict[entry_key[0]]