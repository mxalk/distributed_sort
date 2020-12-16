from multiprocessing import Process
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 
from distributed_sort import Sorter

threads = []
if __name__ == "__main__":
    args = len(sys.argv)
    if args != 2:
        print("python run_sorters N")
    sorters = int(sys.argv[1])
    for i in range(sorters):
        name = "Sorter "+str(i)
        t = Process(name=name, target=Sorter)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()