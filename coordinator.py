import socket
import select
import sys
import logging
from threading import Thread
from threading import Condition
import numpy as np
import time
from datetime import datetime
import random

import utility

BufferSize = utility.parseNumber("64K")
MAGIC = "distributed_sort"
BEACON_PORT = 11338 # UDP
DATA_PORT = 11337 # TCP
LIMIT = 1000
REFRESH_TIME = 0.5
NODE_KEEPALIVE_SOFT_TIMEOUT_SEC = 1.0
NODE_KEEPALIVE_HARD_TIMEOUT_SEC = 5.0
# sequentialReceve = True
sequentialReceve = False
logging.basicConfig(format='%(levelname)s:%(message)s', level = logging.INFO)
# logging.basicConfig(format='%(levelname)s:%(message)s', level = logging.DEBUG)
# unknown c:ready s:data c:busy c:done c:sending c:finished s:closed
# 0       1       2      3      4      5         6          7
STATUS = ["unknown", "ready", "data", "busy", "done", "sending", "finished", "closed"]
class Reader:

    def __init__(self, filename, sorters):
        logging.info("Starting Reader Node")

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
        logging.debug("PROGRAM FINISH")

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
        self.filename_write = filename+'_sorted'
        self.file_write = open(self.filename_write, 'w')
        self.nodes = []
        for i in range(sorters):
            self.nodes.append({"active": False})
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
        logging.info("NodeManager started")
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
            # for each inactive node, index i
            for i in [i for i in range(len(self.nodes)) if not self.nodes[i]["active"]]:
                # replace
                node = None
                while node is None:
                    node = self.getSorterNode(tcp_sock)
                self.nodes[i] = node
                renewed_nodes.append(i)
            # stop beacon
            logging.info("Beacon stopped")
            self.beacon_runs = False
            # put renewed nodes to work
            Thread(name="FileProcessor", target=self.processFile, args=[renewed_nodes]).start()
            # back to sleep
            with self.nodeManager_condition:
                self.nodeManager_condition.wait()

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
        conn.sendall(message)
            
        node = {}
        # GENERAL DATA
        node["buffer"] = []
        node["b_buffer"] = b''
        node["state"] = 0
        node["time"] = datetime.now()
        node["data_sent"] = False
        node["data_received"] = False

        # NETWORK DATA
        conn.setblocking(0)
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
            filename_write = "sorter_" + node["nodeID"] +'.fragment'
            node["filename"] = filename_write
            node["file"] = open(filename_write, 'w')

        # ACTIVATE NODE
        node["active"] = True
        logging.info("Sorter node established: %s tcp:%s udp:%s" % (ip, port_tcp, port_udp))
        return node

    def processFile(self, node_indexes, chunk_size_unparsed='80M'):
        # calculate chunk size in bits
        chunk_size = utility.parseNumber(chunk_size_unparsed)
        # entry size = 100 bytes = 800 bits => entries_per_chunk = chunk_size/entry_size
        entries_per_chunk = chunk_size // 800
        
        entries = []
        for entry in self.file_read:
            entries.append(entry)
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
            # TODO CONN MAY HAVE CLOSED ON ERROR
            node["conn"].sendall(utility.encodeData('FINISHED')+b'\0')
            # notify data is sent
            node["data_sent"] = True
            self.send_message("status", node)

    def listener(self):
        logging.debug("Listener started")
        while not self.program_finished:
            try:
                data, addr = self.udp_sock.recvfrom(BufferSize)
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
            node["time"] = datetime.now()

            if m_type == "state":
                state = int(d["state"])
                if node["state"] < state:
                    logging.debug("State update %s: %d-%s" % (node["nodeID"], state, STATUS[state]))
                    node["state"] = state
                    self.wakeup_watchdog()

                if state == 2 and node["data_sent"]: # data
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
                i = [i for i in range(len(self.nodes)) if self.nodes[i]["active"] and self.nodes[i]["conn"] is conn][0]
                node = self.nodes[i]
                data = ''
                while True:
                    try:
                        node["b_buffer"] += conn.recv(BufferSize)
                    except BlockingIOError as e:
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
                            node["file"].write(entry)
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
                        break
        logging.debug("DataReceiver exited")

    def send_message(self, message, node):
        data = {"type": message}
        self.udp_sock.sendto(utility.encodeData(data), node["udp_send_addr"])
        logging.debug("Sending '%s' to %s" % (message, node["nodeID"]))

    def watchdog(self):
        logging.debug("Watchdog started")
        while not self.program_finished:
            text = ''
            finished = 0
            for node in [node for node in self.nodes if node['active']]:
                nodeID = node["nodeID"]
                state = node["state"]
                status = STATUS[state]
                timediff = utility.getTimeDiff(node["time"])

                if state == 7:
                    finished += 1
                elif timediff >= NODE_KEEPALIVE_HARD_TIMEOUT_SEC:
                    # hard timeout
                    logging.debug("Node %s exceeded hard timeout" % (node["nodeID"]))
                    status = '(timeout)'
                    node["actve"] = False
                    self.wakeup_nodeManager()
                elif timediff >= NODE_KEEPALIVE_SOFT_TIMEOUT_SEC:
                    # soft timeout - request state update
                    logging.debug("Node %s exceeded soft timeout" % (node["nodeID"]))
                    self.send_message("status", node)
                text += "-- %s: %9s " % (nodeID, status)
            logging.info(text)
            self.watchdog_timer = datetime.now()
            if finished == len(self.nodes):
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
        self.watchdog_timer = datetime.now()
        while not self.program_finished:
            timediff = utility.getTimeDiff(self.watchdog_timer)
            sleep_for = REFRESH_TIME-timediff
            if sleep_for <= 0:
                self.wakeup_watchdog()
            else:
                time.sleep(sleep_for)
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
            if len(self.nodes[i]["buffer"]) >= LIMIT:
                self.flush_buffer(i)

    # sends buffer of node i to that node
    def flush_buffer(self, i):
        node = self.nodes[i]
        logging.debug("Sending %d entries to %s" % (len(node["buffer"]), node["nodeID"]))
        node["conn"].sendall(utility.encodeData(node["buffer"])+b'\0')
        node["buffer"] = []


class Sorter:

    def __init__(self):
        logging.info("Starting Sorter Node")

        # UDP SOCKET -> ID
        sock_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock_udp.bind(("0.0.0.0", 0))
        sock_udp.settimeout(NODE_KEEPALIVE_HARD_TIMEOUT_SEC)
        (host, udp_port) = sock_udp.getsockname()
        self.sock_udp = sock_udp
        self.udp_port = udp_port

        while True:
            self.state = 0
            self.await_beacon()
            filename = "coordinator_" + self.coord[0] + "_" + str(self.udp_port) + '.fragment'
            self.f = open(filename, 'w+')
            Thread(target=self.receiver).start()
            self.dispatcher()
            self.sock_tcp.close()
            for thread in self.threads:
                thread.join()
            logging.info("------------------------------------------")

        self.sock_udp.close()

    def await_beacon(self):

        sock_beacon = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
        sock_beacon.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock_beacon.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock_beacon.bind(("", BEACON_PORT))

        # create socket to connect to coordinator

        # receive beacon from Sorter Node -- UDP!!
        while True:
            data, addr = sock_beacon.recvfrom(BufferSize)
            magic_bytes = data.decode()
            (ip, port) = addr
            if magic_bytes != MAGIC:
                continue
            logging.info("Reader Node discovered on %s" % (ip))
            # TCP
            sock_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_tcp.settimeout(5)
            if self.start_connection(ip, sock_tcp):
                break
            sock_tcp.close()
        sock_beacon.close()
        self.sock_tcp = sock_tcp
        self.time = datetime.now()
        self.ok = True
        self.threads = []
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
        self.setState(1)
        while self.ok:
            try:
                data, addr = self.sock_udp.recvfrom(BufferSize)
            except socket.timeout as e:
                logging.info("Hard timeout! Resetting")
                self.ok = False
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
            self.time = datetime.now()

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
                logging.info("Close command received")
                self.setState(7)
                break

    def receiver(self):
        b_buffer = b''
        data = ""
        sock_tcp = self.sock_tcp
        while self.ok:
            try:
                b_buffer += sock_tcp.recv(BufferSize)
            except socket.timeout:
                continue
            except ConnectionResetError as e:
                self.ok = False
                continue
            except OSError as e: # socket closed
                self.ok = False
                continue
            except Exception as e:
                raise e
            while b'\0' in b_buffer:
                b_data, ignored, b_buffer = b_buffer.partition(b'\0')
                data = utility.decodeData(b_data)
                if data == "FINISHED":
                    break
                logging.debug("Received %d entries" % (len(data)))
                for entry in data:
                    self.f.write(entry)
            if data == "FINISHED":
                self.setState(2)
                break

    def sorter(self):
        logging.info("Sorting started")
        # SORT DATA
        self.f.seek(0)
        data = []
        for entry in self.f:
            data.append(entry)
        self.sorted_data = sorted(data)
        # sorted_data = self.radix_sort(self.data)
        logging.info("Sorting finished")
        self.setState(4)

    def sender(self):
        logging.info("Sending started")
        # SEND DATA
        self.buffer = []
        for entry in self.sorted_data:
            self.buffer.append(entry)
            if len(self.buffer) >= LIMIT:
                self.flush_buffer()
        if len(self.buffer) > 0:
            self.flush_buffer()
        self.sock_tcp.sendall(utility.encodeData("FINISHED")+b'\0')
        logging.info("Sending finished")
        self.setState(6)
    
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

    def radix_sort(self, data):
        char_list = [chr(x) for x in range(32,127)]
        max_length = 4 ### compare first 4 characters

        for i in range(max_length):
            bucket = {}
            for char in char_list:
                bucket[ord(char)] = []
            for tuple in data:
                temp_word = tuple[0:4]
                bucket[ord(temp_word[max_length - i - 1])].append(tuple)
    
            index = 0
            for char in char_list:
                if (len(bucket[ord(char)])) != 0:
                    for i in bucket[ord(char)]:
                        data[index] = i
                        index = index + 1
        return data