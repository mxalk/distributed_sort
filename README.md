<h1> Distributed Sort </h1>

Distributed sorting implementated in python.

Python scripts *coordinator.py*, *sorter.py* in experiments/ are wrappers for distributed_sort.py handling arguments and creating the corresponding class.

Usage:  
&nbsp;&nbsp;&nbsp;&nbsp;python3 *coordinator.py* [filename] [sorters] [nodelist (optional)]  
&nbsp;&nbsp;&nbsp;&nbsp;python3 *sorter.py*  

[filename]: the unsorted dataset. gensort ASCII format is expected.  
[sorters]: number of sorters to use.  
[nodelist]: new line separated file of node IPs to run in datacenter mode, where broadcast for auto discovery is not available.  
  
SLURM template for sorter batch deployment is also available.
