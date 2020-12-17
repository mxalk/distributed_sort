char_space = ": !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
all_chars = ""
for char_i in char_space:
    for char_j in char_space:
        char_pair = char_i + "" + char_j
        all_chars+=(char_pair)
for n_partitions in range(1, 40): 
    # Split the array in n roughly equal size parts
    chars_per_array = len(all_chars) // n_partitions +1
    char_array_split_parts = []
    i=0
    for i in range(n_partitions-1):
        part = all_chars[i*chars_per_array:(i+1)*chars_per_array]
        char_array_split_parts.append(part)
    part = all_chars[(len(all_chars)//chars_per_array)*chars_per_array:]
    char_array_split_parts.append(part)

    # # Create a dictionary which has as key the characters
    # # and as value the partition it belongs to.
    # for i in range(len(char_array_split_parts)):
    #     partition = char_array_split_parts[i]
    #     for char in partition:
    #         self.partition_dict[char] = int(i)
    
    print("Partitions: %d: %s" % (n_partitions, ', '.join(map(str, [len(item) for item in char_array_split_parts]))))
    # for i in range(len(char_array_split_parts)):
    #     arr = char_array_split_parts[i]
    #     print("\t%5d) %s" % (i, ' '.join(arr)))