import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 
import distributed_sort

filename = "/tmp/file.dat"
# filename = "file.dat"

reader = distributed_sort.Reader(filename, 5)