import sys

def is_sorted(sorted_filename):
    datafile = None
    try:
        datafile = open(sorted_filename, "r")
    except:
        print("File not found")
        return
    print("Starting sorting validation...")

    line_nr = 1

    prev_line = datafile.readline()
    cur_line = datafile.readline()
    while True:
        prev_key = prev_line[:10]
        cur_key = cur_line[:10]

        if not prev_key < cur_key:
            print("Unsorted element in line: " + str(line_nr))
            print(cur_key + " is smaller than " + prev_key)
            return 

        prev_line = cur_line
        cur_line = datafile.readline()
        line_nr += 1
        if not cur_line:
            break

    print("File correctly sorted!")
    datafile.close()
    return

def is_malformed(sorted_filename, unsorted_filename):
    try:
        datafile_sorted = open(sorted_filename, "rb")
    except:
        print("Sorted file not found")
        return
    try:
        datafile_unsorted = open(unsorted_filename, "rb")
    except:
        print("Unsorted file not found")
        return
    print("Starting content validation...")
    
    xor_sorted = datafile_sorted.readline()[:10]
    xor_unsorted = datafile_unsorted.readline()[:10]

    line = datafile_sorted.readline()[:10]
    while line:
        xor_sorted = bytes(a ^ b for (a, b) in zip(xor_sorted, line))
        line = datafile_sorted.readline()[:10]
    print("Sorted XOR:")
    for i in range(len(xor_sorted)):
        if i%10 == 0:
            print("\t", end="")
        byte = xor_sorted[i]
        print("%3s " % (str(byte)), end="")
        if i%10 == 9:
            print("")

    line = datafile_unsorted.readline()[:10]
    while line:
        xor_unsorted = bytes(a ^ b for (a, b) in zip(xor_unsorted, line))
        line = datafile_unsorted.readline()[:10]
    print("Unsorted XOR:")
    for i in range(len(xor_unsorted)):
        if i%10 == 0:
            print("\t", end="")
        byte = xor_unsorted[i]
        print("%3s " % (str(byte)), end="")
        if i%10 == 9:
            print("")

    if xor_sorted == xor_unsorted:
        print("File contents match!")
    else:
        print("File contents corrupted!")

if __name__ == "__main__":
    args = len(sys.argv)
    if args >= 2 and args <= 3:
        sorted_filename = str(sys.argv[1])
        is_sorted(sorted_filename)
        if args == 3:
            unsorted_filename = str(sys.argv[2])
            is_malformed(sorted_filename, unsorted_filename)
    else:
        print("Usage: python3 validator.py [sorted filename] [unsorted filename (optional)]")

