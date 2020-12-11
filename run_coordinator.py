import distributed_sort

filename = "/tmp/file.dat"
# filename = "file.dat"

reader = distributed_sort.Reader(filename, 4)