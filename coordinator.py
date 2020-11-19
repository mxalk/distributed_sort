import socket
import sys
import logging

import utility

BufferSize = 4096
MAGIC = "distributed_sort"

class Reader:

    def __init__(filename, min_nodes):

        self.init_variables()
        self.setup_nodes(min_nodes)
        self.readFile(filename)

    def init_variables():

        self.nodes = {}
        self.connections = []

    def setup_nodes(min_nodes):
        # create socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
        # wait for min_nodes incomming beacons and register them
        while len(self.nodes) < min_nodes:
            self.await_connection(sock)
        # establish connection to nodes


    def await_connection(sock):
        # receive beacon from Sorter Node -- UDP!!
        data, addr = sock.recvfrom(BufferSize)
        # data format: #magic_bytes#memory_avaible
        magic_bytes = data[:16]
        if magic_bytes != MAGIC or addr in self.nodes:
            return
        memory_avaible = data[17:]
        self.nodes[addr] = memory_avaible
        logging.info("Sorter Node discovered on %s, RAM: %s" % (addr, memory_avaible))

    def readFile(filename, chunk_size_unparsed='80M'):

        # calculate chunk size in bits
        chunk_size = utility.parseNumber(chunk_size_unparsed)
        # entry size = 100 bytes = 800 bits => entries_per_chunk = chunk_size/entry_size
        entries_per_chunk = chunk_size // 800
        
        f = open(filename)
        entries = []
        for entry in f:
            entries.append(entry)
            if len(entries) < entries_per_chunk:
                continue
            self.process_entries(entries)
            entries = []
        self.process_entries(entries)

    # receives an entries[] of size chunk_size_unparsed
    def process_entries(entries):
        pass

    def choose_partition(entry, n_partitions):
        # TODO
        pass
        # return which partition the entry needs to be send to 



"""
Phase 1 
"""

# Setup:
# Decide on amount of nodes
# Then partition keyspace for that amount of nodes

# Readers:
# Input The data file that needs to be sorted
# Read in small chunks of like 80 mbs
# f = f.open("data")
# for chunk in f:
#   for entry in chunk:
#       Decide to which partition the entry needs to be send.


# partion buffer part
# contains for each partition entries of the datafile that later will be send 
# to the corresponding node when the buffer is full.

# Buffer sender 
# Simply sends a buffer to a partition

# Buffer receiver
# receives a buffer and writes it to the harddisk


"""
Phase 2
"""

# sorting on each partition

# resend the sorted partitions

# merge the sorted partitions

class Sorter:

    def start_connection():
        # TODO
        # open port

    def quicksort(arr):
        # TODO
        # implementation of sorting algorithm  