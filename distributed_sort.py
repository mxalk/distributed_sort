import socket
import select
import sys
import logging
from threading import Thread
from threading import Condition
import numpy as np
import time
import random

import sorting_algorithms
import utility

# --------------------------------------------------------------------------------------------------------------------------
# -------     SETTINGS     -------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------------
# NETWORK
MAGIC = "distributed_sort"
BEACON_PORT = 11338 # UDP
DATA_PORT = 11337 # TCP
# BUFFERS
READER_BUFFERS_SIZE = "5M"
SORTER_BUFFER_SIZE = "5M"
NET_BUFFER_SIZE = "64K"
# TIMING
REFRESH_TIME = 1
NODE_KEEPALIVE_SOFT_TIMEOUT_SEC = 2.0
NODE_KEEPALIVE_HARD_TIMEOUT_SEC = 10.0
# BEHAVIOR
sorterSavesData = False     # default False
sequentialReceve = False    # default False
# LOGGING
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level = logging.INFO, datefmt='%H:%M:%S')
# logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level = logging.DEBUG, datefmt='%H:%M:%S')

# CONSTANTS
# ready c:accepted s:data_recv c:busy c:done c:sending c:data_sent s:closed c:timeout c:failed c:replacing
# 0     1          2           3      4      5         6          7         10        11       12
STATUS = ["ready", "accepted", "data_recv", "busy", "done", "sending", "data_sent", "closed", "", "", "timeout", "failed", "replacing"]
READER_BUFFERS_ENTRIES_SIZE = utility.parseNumber(READER_BUFFERS_SIZE) // 100
SORTER_BUFFERS_ENTRIES_SIZE = utility.parseNumber(SORTER_BUFFER_SIZE) // 100
NET_BUFFER_SIZE_PARSED = utility.parseNumber(NET_BUFFER_SIZE)
# --------------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------------

class Reader:

    def __init__(self, filename, sorters):
        logging.info("Starting Reader Node")
        self.time = {}
        self.time["program_start"] = time.perf_counter()

        self.init_variables(filename, sorters)
        self.create_partition_dict(sorters)

        threads = []
        threads.append(Thread(name="Watchdog", target=self.watchdog))
        threads.append(Thread(name="RefreshLoop", target=self.refresh_loop))
        threads.append(Thread(name="DataReceiver", target=self.data_receiver))
        threads.append(Thread(name="Listener", target=self.listener))
        threads.append(Thread(name="NodeManager", target=self.nodeManager))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # merge files
        if not sequentialReceve:
            logging.info("Merging files")
            for filename in [node["filename"] for node in self.nodes]:
                f_node = open(filename, 'r')
                for entry in f_node:
                    self.file_write.write(entry)

        for conn in [self.nodes[i]["conn"] for i in range(len(self.nodes))]:
            conn.close()
        self.time["program_finish"] = time.perf_counter()
        logging.debug("PROGRAM FINISH")
        logging.info("Total run time: %.2f" % (self.time["program_finish"]-self.time["program_start"]))

    def init_variables(self, filename, sorters):

        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        udp_sock.bind(("0.0.0.0", DATA_PORT))
        udp_sock.setblocking(0)
        udp_sock.settimeout(1)
        self.udp_sock = udp_sock
        
        self.program_finished = False
        self.beacon_runs = False
        self.filename_read = filename
        self.file_read = open(self.filename_read, 'r')
        self.file_read_entries_read = -1
        self.filename_write = filename+'_sorted'
        self.file_write = open(self.filename_write, 'w')
        self.nodes = []
        self.nodes_to_replace = set()
        for i in range(sorters):
            self.nodes.append({"active": False, "state": -1})
            self.nodes_to_replace.add(i)
        self.partition_dict = {}
        self.addr_map = {}
        self.watchdog_condition = Condition()
        self.watchdog_timer = None
        self.nodeManager_condition = Condition()
        self.data_from = 0

    def beacon(self):
        logging.info("Beacon started")
        message = MAGIC.encode()
        self.beacon_runs = True
        try:
            while self.beacon_runs:
                self.udp_sock.sendto(message, ('<broadcast>', BEACON_PORT))
                logging.debug("Beacon sent")
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logging.error(e)

    def nodeManager(self):
        logging.debug("NodeManager started")
        # create socket to wait for sorter nodes
        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        try:
            tcp_sock.bind(("0.0.0.0", DATA_PORT))
        except OSError as e:
            raise e
        tcp_sock.listen()
        # tcp_sock.settimeout(10)

        while not self.program_finished:
            renewed_nodes = []
            # start beacon
            Thread(target=self.beacon).start()
            # for each node scheduled for replacement
            while len(self.nodes_to_replace) != 0:
                i = list(self.nodes_to_replace)[0]
                # replace
                node = None
                while node is None:
                    node = self.getSorterNode(tcp_sock)
                self.nodes[i] = node
                renewed_nodes.append(i)
                self.nodes_to_replace.pop()
            # stop beacon
            logging.info("Beacon stopped")
            self.beacon_runs = False
            # put renewed nodes to work
            Thread(name="FileProcessor", target=self.processFile, args=[renewed_nodes]).start()
            # back to sleep
            with self.nodeManager_condition:
                self.nodeManager_condition.wait()
            logging.debug("NodeManager woken")

        tcp_sock.close()
        logging.debug("NodeManager exited")
    
    def wakeup_nodeManager(self):
        with self.nodeManager_condition:
            self.nodeManager_condition.notify_all()

    def getSorterNode(self, tcp_sock):
        try:
            conn, addr_tcp = tcp_sock.accept()
        # except socket.timeout:
        #     logging.debug("Accept timeout")
        #     continue
        except KeyboardInterrupt as e:
            raise e
        except Exception as e:
            raise e
        logging.info("Incoming connection from %s:%s" % (addr_tcp))
        data = conn.recv(len(MAGIC)+5).decode()
        # verify sorter is connecting
        if MAGIC != data[:len(MAGIC)]:
            logging.debug("Sorter node failed MAGIC: %s:%s" % (addr_tcp))
            conn.close()
            return None
        # get sorter udp port
        if len(data) < len(MAGIC)+5:
            logging.debug("Sorter node failed due to packet size: %s:%s" % (addr_tcp))
        (ip, port_tcp) = addr_tcp
        port_udp = int(data[len(MAGIC):])
        nodeID = ip+':'+str(port_tcp)+':'+str(port_udp)
        # verify sorter is not already connected
        if (ip, port_udp) in [(node["ip"], node["port_udp"]) for node in self.nodes if node["active"]]:
            logging.debug("Sorter node instance exists: %s" % (nodeID))
            conn.close()
            return None


        message = (MAGIC).encode()
        try:
            conn.sendall(message)
        except Exception as e:
            return None
            
        node = {}
        # GENERAL DATA
        node["buffer"] = []
        node["b_buffer"] = b''
        node["state"] = 0
        node["time"] = time.perf_counter()
        node["data_received"] = False

        # NETWORK DATA
        conn.setblocking(1)
        node["conn"] = conn
        self.addr_map[(ip, port_udp)] = node
        node["ip"] = ip
        node["port_udp"] = port_udp
        node["port_tcp"] = port_tcp
        node["nodeID"] = nodeID
        node["udp_send_addr"] = (ip, port_udp)

        # FILE DATA
        node["file"] = self.file_write
        if not sequentialReceve:
            filename_write = self.filename_read + "_sorter_" + node["nodeID"] +'.fragment'
            node["filename"] = filename_write
            node["file"] = open(filename_write, 'w')

        # ACTIVATE NODE
        node["active"] = True
        logging.info("Sorter node established: %s tcp:%s udp:%s" % (ip, port_tcp, port_udp))
        return node

    def processFile(self, node_indexes, chunk_size_unparsed='80M'):
        logging.debug("processFile started for nodes: %s" % (str(node_indexes)))
        # calculate chunk size in bits
        chunk_size = utility.parseNumber(chunk_size_unparsed)
        # entry size = 100 bytes = 800 bits => entries_per_chunk = chunk_size/entry_size
        entries_per_chunk = chunk_size // 800
        
        entries = []
        self.file_read.seek(0)
        self.file_read_entries_read = 0
        for entry in self.file_read:
            entry = entry.replace('\n', '')
            entries.append(entry)
            self.file_read_entries_read += 1
            if len(entries) < entries_per_chunk:
                continue
            self.process_entries(entries, node_indexes)
            entries = []
        self.process_entries(entries, node_indexes)

        # flush buffers
        for i in node_indexes:
            node = self.nodes[i]
            if len(node["buffer"]) > 0:
                self.flush_buffer(i)
                # finalize
            try:
                node["conn"].sendall(utility.encodeData('FINISHED')+b'\0')
            except BrokenPipeError as e:
                logging.debug(e)
                node['active'] = False
                node["state"] = 11
                continue
            # notify data is sent
            node["conn"].setblocking(0)
            self.send_message("status", node)

    def listener(self):
        logging.debug("Listener started")
        while not self.program_finished:
            try:
                data, addr = self.udp_sock.recvfrom(NET_BUFFER_SIZE_PARSED)
            except socket.timeout as e:
                continue
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                raise e
            node = self.addr_map.get(addr)
            if node is None:
                logging.debug("Data from unknown source: %s: %s" % (addr, data))
                continue
            d = utility.decodeData(data)
            m_type = d.get("type")
            if m_type is None:
                logging.debug("Message from %s:%s does not contain type" % (addr))
                continue
            
            nodeID = node["nodeID"]
            node["time"] = time.perf_counter()

            if m_type == "state":
                state = int(d["state"])
                if node["state"] < state:
                    logging.debug("State update %s: %d-%s" % (node["nodeID"], state, STATUS[state]))
                    node["state"] = state
                    self.wakeup_watchdog()

                if state == 2: # data
                    self.send_message("sort", node)
                    continue

                if state == 4: # done
                    if sequentialReceve and self.nodes.index(node) != self.data_from:
                        continue
                    self.send_message("send", node)
                    continue

                if state == 6 and node["data_received"]: # finished
                    self.send_message("close", node)
                    continue

                if state == 7: # closed
                    logging.debug("Remote connection closed: %s" % (node["nodeID"]))
                    continue

        logging.debug("Listener exited")
    
    def data_receiver(self):
        logging.debug("DataReceiver started")
        while not self.program_finished:
            active_conns = [node["conn"] for node in self.nodes if node["active"] and not node["data_received"]]
            try:
                readable, writable, errored = select.select(active_conns, [], [], 1)
            except ValueError as e: # race condition where conn has already closed
                time.sleep(0.1)
                continue
            for conn in readable:
                try:
                    i = [i for i in range(len(self.nodes)) if self.nodes[i]["active"] and self.nodes[i]["conn"] is conn][0]
                except IndexError as e: # timeout is recent and not yet updated
                    continue
                node = self.nodes[i]
                data = ''
                try:
                    node["b_buffer"] += conn.recv(NET_BUFFER_SIZE_PARSED)
                except ConnectionResetError as e:
                    node['active'] = False
                    node["state"] = 11
                    continue
                except Exception as e:
                    raise e
                while b'\0' in node["b_buffer"]:
                    b_data, ignored, node["b_buffer"] = (node["b_buffer"]).partition(b'\0')
                    data = utility.decodeData(b_data)
                    if data == "FINISHED":
                        break
                    logging.debug("Received %d entries from node %s" % (len(data), node["nodeID"]))
                    for entry in data:
                        node["file"].write(entry+'\n')
                if data == "FINISHED":
                    logging.debug("Node %s finished" % (node["nodeID"]))
                    self.data_from += 1
                    self.send_message("status", node)
                    if sequentialReceve:
                        if i+1 < len(self.nodes):
                            self.send_message("status", self.nodes[i+1])
                    else:
                        node["file"].close()
                    node["data_received"] = True
                    conn.close()
                    # break
        logging.debug("DataReceiver exited")

    def send_message(self, message, node):
        data = {"type": message}
        self.udp_sock.sendto(utility.encodeData(data), node["udp_send_addr"])
        logging.debug("Sending '%s' to %s" % (message, node["nodeID"]))

    def watchdog(self):
        logging.debug("Watchdog started")
        while not self.program_finished:
            text = 'STATUS: '
            if self.file_read_entries_read != -1:
                text += 'lines read: %d' % (self.file_read_entries_read)
            finished = 0
            for i in [i for i in range(len(self.nodes)) if self.nodes[i]["state"] != -1]:
                node = self.nodes[i]
                nodeID = node["nodeID"]
                timediff = time.perf_counter() - node["time"]

                if node["state"] == 7:
                    finished += 1
                elif node["state"] in [10, 11, 12]:
                    pass
                elif timediff >= NODE_KEEPALIVE_HARD_TIMEOUT_SEC:
                    if not node["data_received"]:
                        # hard timeout
                        logging.debug("Node %s exceeded hard timeout" % (node["nodeID"]))
                        node["active"] = False
                        node["state"] = 10
                    else: # data received => state == 6 // remote connection must have closed
                        node["state"] = 7
                elif timediff >= NODE_KEEPALIVE_SOFT_TIMEOUT_SEC:
                    # soft timeout - request state update
                    # logging.debug("Node %s exceeded soft timeout" % (node["nodeID"]))
                    self.send_message("status", node)
                text += "\n %s: %9s (%d) | %.2f - " % (nodeID, STATUS[node["state"]], node["state"], timediff)
            self.watchdog_timer = time.perf_counter()
            logging.info(text)
            to_replace = [i for i in range(len(self.nodes)) if self.nodes[i]["state"] in [10, 11]]
            if len(to_replace) != 0:
                for i in to_replace:
                    self.nodes[i]["state"] = 12
                    self.nodes_to_replace.add(i)
                self.wakeup_nodeManager()
            if finished == len(self.nodes):
                logging.debug("Program is finished")
                self.program_finished = True
                self.wakeup_nodeManager()
                break
            with self.watchdog_condition:
                self.watchdog_condition.wait()
            logging.debug("Watchdog woken")
        logging.debug("Watchdog exited")

    def wakeup_watchdog(self):
        with self.watchdog_condition:
            self.watchdog_condition.notify_all()

    def refresh_loop(self):
        logging.debug("RefreshLoop started")
        self.watchdog_timer = time.perf_counter()
        while not self.program_finished:
            timediff = time.perf_counter() - self.watchdog_timer
            sleep_for = REFRESH_TIME-timediff
            if sleep_for <= 0:
                self.wakeup_watchdog()
            else:
                time.sleep(sleep_for)
            time.sleep(0.01)
        logging.debug("RefreshLoop exited")

    def create_partition_dict(self, n_partitions):
        # The characters which keys consists of.
        char_space = ": !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
        
        # Create a array of all characters
        char_array = [char for char in char_space]

        # Split the array in n roughly equal size parts
        char_array_split_parts = np.array_split(char_array, n_partitions)

        # Create a dictionary which has as key the characters
        # and as value the partition it belongs to.
        for i in range(len(char_array_split_parts)):
            partition = char_array_split_parts[i]
            for char in partition:
                self.partition_dict[char] = int(i)
    
    # receives an entries[] of size chunk_size_unparsed
    def process_entries(self, entries, node_indexes):
        for entry in entries:
            i = self.partition_dict[entry[0]]
            if i not in node_indexes:
                continue
            self.nodes[i]["buffer"].append(entry)
            if len(self.nodes[i]["buffer"]) >= READER_BUFFERS_ENTRIES_SIZE:
                if not self.flush_buffer(i):
                    node_indexes.remove(i)

    # sends buffer of node i to that node
    def flush_buffer(self, i):
        success = True
        node = self.nodes[i]
        logging.debug("Sending %d entries to %s" % (len(node["buffer"]), node["nodeID"]))
        try:
            node["conn"].sendall(utility.encodeData(node["buffer"])+b'\0')
        except Exception as e:
            logging.debug(e)
            node["active"] = False
            node["state"] = 11
            success = False
        node["buffer"] = []
        return success


class Sorter:

    def __init__(self):
        logging.info("Starting Sorter Node")


        while True:
            self.state = 0
            self.await_beacon()
            filename = "coordinator_" + self.coord[0] + "_" + str(self.udp_port) + '.fragment'
            if sorterSavesData:
                self.f = open(filename, 'w+')
            t = Thread(target=self.receiver)
            self.threads.append(t)
            t.start()
            self.dispatcher()
            self.sock_tcp.close()
            for thread in self.threads:
                thread.join()
            logging.info("------------------------------------------")

        self.sock_udp.close()

    def await_beacon(self):
        # UDP SOCKET -> ID
        sock_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock_udp.bind(("0.0.0.0", 0))
        sock_udp.settimeout(NODE_KEEPALIVE_HARD_TIMEOUT_SEC)
        (host, udp_port) = sock_udp.getsockname()
        self.sock_udp = sock_udp
        self.udp_port = udp_port

        sock_beacon = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
        sock_beacon.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock_beacon.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock_beacon.bind(("", BEACON_PORT))

        # create socket to connect to coordinator

        # receive beacon from Sorter Node -- UDP!!
        while True:
            data, addr = sock_beacon.recvfrom(NET_BUFFER_SIZE_PARSED)
            magic_bytes = data.decode()
            (ip, port) = addr
            if magic_bytes != MAGIC:
                continue
            logging.info("Reader Node discovered on %s" % (ip))
            # TCP
            sock_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_tcp.settimeout(5)
            sock_tcp.setblocking(1)
            if self.start_connection(ip, sock_tcp):
                break
            sock_tcp.close()
        sock_beacon.close()
        self.sock_tcp = sock_tcp
        self.ok = True
        self.threads = []
        self.unsorted_data = []
        logging.info("Connection established")

    def start_connection(self, ip, sock_tcp):
        try:
            sock_tcp.connect((ip, DATA_PORT))
            message = (MAGIC+str(self.udp_port)).encode()
            sock_tcp.sendall(message)
            data = sock_tcp.recv(len(MAGIC)).decode()
        except Exception as e: # ConnectionResetError when coordinator chooses other node
            logging.debug(e)
            return False
        if data != MAGIC:
            return False
        self.coord = (ip, DATA_PORT)
        return True

    def dispatcher(self):
        self.setState(0)
        while self.ok:
            try:
                data, addr = self.sock_udp.recvfrom(NET_BUFFER_SIZE_PARSED)
            except socket.timeout as e:
                logging.info("Hard timeout! Resetting")
                self.ok = False
                self.sock_tcp.close()
                continue
            except Exception as e:
                raise e
            
            if addr != self.coord:
                logging.debug("Data from unknown source: %s:%s: %s" % (addr, data))
                continue
            d = utility.decodeData(data)
            m_type = d.get("type")
            if m_type is None:
                logging.debug("Message does not contain type" % (addr))
                continue
            logging.debug("Refresh time: %.2f" % time.perf_counter())

            if m_type == "sort":
                if self.state != 2:
                    logging.debug("state: %d, 2 expected" % (self.state))
                    continue

                logging.info("Sort command received")
                self.setState(3)
                # Start sorting process
                t = Thread(name="Sorter", target=self.sorter)
                self.threads.append(t)
                t.start()

            if m_type == "status":
                logging.debug("Status command received")
                self.stateUpdate()

            if m_type == "send":
                if self.state != 4:
                    logging.debug("state: %d, 4 expected" % (self.state))
                    continue

                logging.info("Send command received")
                self.setState(5)
                # Start sending process
                t = Thread(name="Sender", target=self.sender)
                self.threads.append(t)
                t.start()

            if m_type == "close":
                if self.state != 6:
                    logging.debug("state: %d, 6 expected" % (self.state))
                    continue
                logging.info("Close command received")
                self.setState(7)
                break

    def receiver(self):
        b_buffer = b''
        data = ""
        sock_tcp = self.sock_tcp
        while self.ok:
            try:
                b_buffer += sock_tcp.recv(NET_BUFFER_SIZE_PARSED)
            except socket.timeout:
                continue
            except ConnectionResetError as e:
                logging.debug("TCP Connection reset")
                self.ok = False
                continue
            except OSError as e: # socket closed
                logging.debug("TCP Socket closed")
                self.ok = False
                continue
            except Exception as e:
                raise e
            while b'\0' in b_buffer:
                b_data, ignored, b_buffer = b_buffer.partition(b'\0')
                data = utility.decodeData(b_data)
                self.state = 1 # do not self.stateUpdate(), causes timeout due to coordinator not probing
                if data == "FINISHED":
                    break
                logging.debug("Received %d entries" % (len(data)))
                self.unsorted_data += data
                if sorterSavesData:
                    for entry in data:
                        self.f.write(entry+'\n')
            if data == "FINISHED":
                self.setState(2)
                break

    def sorter(self):
        logging.info("Sorting started")
        # SORT DATA
        # self.sorted_data = sorting_algorithms.insertion_sort(self.unsorted_data)
        self.sorted_data = sorting_algorithms.radix_sort(self.unsorted_data, 10)
        # self.sorted_data = sorting_algorithms.quicksort(self.unsorted_data)
        # self.sorted_data = sorted(self.unsorted_data)
        logging.info("Sorting finished")
        self.setState(4)

    def sender(self):
        logging.info("Sending started")
        # SEND DATA
        self.buffer = []
        try:
            for entry in self.sorted_data:
                self.buffer.append(entry)
                if len(self.buffer) >= SORTER_BUFFERS_ENTRIES_SIZE:
                    self.flush_buffer()
            if len(self.buffer) > 0:
                self.flush_buffer()
            self.sock_tcp.sendall(utility.encodeData("FINISHED")+b'\0')
            logging.info("Sending finished")
            self.setState(6)
        except OSError as e:
            logging.debug("TCP Connection closed")
            self.ok = False
        except Exception as e:
            logging.debug(e)
            self.ok = False


    def flush_buffer(self):
        logging.debug("Sending %d entries" % (len(self.buffer)))
        self.sock_tcp.sendall(utility.encodeData(self.buffer)+b'\0')
        self.buffer = []

    def setState(self, state):
        self.state = state
        self.stateUpdate()

    def stateUpdate(self):
        d = {"type": "state", "state" : self.state}
        logging.debug("Sending state update %d-%s" % (self.state, STATUS[self.state]))
        self.sock_udp.sendto(utility.encodeData(d), self.coord)

    def stateUpdater(self):
        while self.stateUpdater_run:
            self.stateUpdate()
            time.sleep(1)