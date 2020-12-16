import sys
import sorting_algorithms
import time

f = 'file.dat'


def openFile(filename):
    try:    
        with open(f,'r') as raw_input:
            data = raw_input.read().strip()
            arr = data.split("\n")
    except FileNotFoundError:
        print('File not found')
        return None
    return arr


def bench(func, arr):
    start = time.perf_counter()
    sorted_arr = func(arr)
    end = time.perf_counter()
    return (sorted_arr, end-start)


if __name__ == "__main__":

    arr = openFile(f)
    if arr is not None:

        # (sorted_arr, tdiff) = bench(sorting_algorithms.insertion_sort, arr)
        # print("Insertion Sort: %f" % (tdiff))

        (sorted_arr, tdiff) = bench(sorting_algorithms.radix_sort, arr)
        print("Radix Sort: %f" % (tdiff))

        # (sorted_arr, tdiff) = bench(sorting_algorithms.quickSort, arr)
        # print("QuickSort: %f" % (tdiff))

        # (sorted_arr, tdiff) = bench(sorted, arr)
        # print("Sorted: %f" % (tdiff))

        # print(sorted_arr)
        for item in sorted_arr:
            print(item)