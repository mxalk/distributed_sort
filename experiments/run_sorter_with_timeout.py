from multiprocessing import Process
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 
from distributed_sort import Sorter

TIMEOUT = 300
if __name__ == "__main__":
    t = Process(target=Sorter)
    t.start()
    sleep(TIMEOUT)
    t.kill()