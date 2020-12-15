import sys

def main(argv):
    datafile = None
    try:
        datafile = open(argv, "r")
    except:
        print("File not found")
    print("File found")
    print("Now starting validation...")

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

    print("File correctly sorted")
    datafile.close()
    return

if __name__ == "__main__":
    try:
        argv = str(sys.argv[1])
        main(argv)
    except IndexError as error:
        print("Error no file was found.")
        print("Usage: python3 validator.py [filename]")
    
