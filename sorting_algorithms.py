import logging


# INSERTION SORT
def insertion_sort(arr):
    for i in range(1, len(arr)):
        current_value = arr[i] 
        position = i   
        while position > 0 and arr[position-1][0:4] > current_value[0:4]:
                arr[position] = arr[position-1]
                position = position - 1
        arr[position] = current_value
    return arr

# RADIX SORT
def radix_sort(arr, max_length=10):
    char_list = [chr(x) for x in range(32,127)]
    for i in range(max_length):
        logging.info("Radix sort iteration: %d/%d" % (i+1, max_length))
        bucket = {}
        for char in char_list:
            bucket[ord(char)] = []
        for item in arr:
            temp_word = item[:max_length]
            bucket[ord(temp_word[max_length - i - 1])].append(item)
        index = 0
        for char in char_list:
            if (len(bucket[ord(char)])) != 0:
                for i in bucket[ord(char)]:
                    arr[index] = i
                    index = index + 1
    return arr

# QUICKSORT
def partition(arr, low, high):
    i = (low-1)
    pivot = arr[high]
    for j in range(low, high):
        if arr[j] <= pivot:
            i = i+1
            arr[i], arr[j] = arr[j], arr[i]
    arr[i+1], arr[high] = arr[high], arr[i+1]
    return (i+1)
def quickSortNested(arr, low, high):
    if len(arr) == 1:
        return arr
    if low < high:
        pi = partition(arr, low, high)
        quickSortNested(arr, low, pi-1)
        quickSortNested(arr, pi+1, high)
def quickSort(arr):
    if type(arr) != list:
        raise Exception
    if len(arr) == 1:
        return arr
    low = 0
    high = len(arr)-1
    pi = partition(arr, low, high)
    quickSortNested(arr, low, pi-1)
    quickSortNested(arr, pi+1, high)