import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 
from distributed_sort import Reader

def usage():
    print("python3 coordinator.py [filename] [sorters] [nodelist (optional)]")

if __name__ == "__main__":
    args = len(sys.argv)
    nodelist = None
    if args > 4 or args < 3:
        usage()
        exit()
    if args == 4:
        nodelist = sys.argv[3]
    try:
        nodes = int(sys.argv[2])
    except Exception as e:
        usage()
        print("\t[sorters] must be a number!")
        exit()
    filename = sys.argv[1]
    Reader(filename, nodes, nodelist)