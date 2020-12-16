import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 
from distributed_sort import Sorter

if __name__ == "__main__":
    args = len(sys.argv)
    if args != 1:
        print("python3 sorter.py")
        exit()
    Sorter()