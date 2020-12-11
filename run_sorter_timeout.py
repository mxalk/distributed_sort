import distributed_sort
from multiprocessing import Process

p = Process(target=distributed_sort.Sorter())
sleep(10)
p.kill()
