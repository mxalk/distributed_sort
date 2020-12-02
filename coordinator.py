import socket
import select
import sys
import logging
from threading import Thread
from threading import Condition
from multiprocessing import Process
import numpy as np
import time
from datetime import datetime
import random

import utility

BufferSize = utility.parseNumber("64K")
MAGIC = "distributed_sort"
BEACON_PORT = 11338 # UDP
DATA_PORT = 11337 # TCP
LIMIT = 500
REFRESH_TIME = 0.5
NODE_KEEPALIVE_SOFT_TIMEOUT_SEC = 1
NODE_KEEPALIVE_HARD_TIMEOUT_SEC = 5.0
uniqueAddress = False
sequentialReceve = True
# sequentialReceve = False
# logging.basicConfig(format='%(levelname)s:%(message)s', level = logging.INFO)
logging.basicConfig(format='%(levelname)s:%(message)s', level = logging.DEBUG)
# unknown c:ready s:data c:busy c:done c:sending c:finished s:close
# 0       1       2      3      4      5         6          7
STATUS = ["unknown", "ready", "data", "busy", "done", "sending", "finished", "close"]
class Reader:

    def __init__(self, filename, sorters):
        logging.info("Starting Reader Node")

        self.init_variables(filename, sorters)

        beacon_thread = Process(target=self.beacon)
        beacon_thread.start()
        self.nodes = self.connectionListener(sorters)
        beacon_thread.kill()

        files = []
        for node in self.nodes:
            if sequentialReceve:
                new_filename = filename + "_sorted"
            else:
                new_filename = filename + "_" + node["nodeID"] + "_sorted"
                files.append(new_filename)
            node["file"] = open(new_filename, 'w')

        logging.info("Beacon stopped")

        self.create_partition_dict(len(self.nodes))

        Thread(target=self.processFile).start()
        Thread(target=self.data_receiver).start()
        self.listener()
        # try:
        #     self.listener()
        # except KeyboardInterrupt:
        #     pass
        # except Exception as e:
        #     logging.error(e)
        if not sequentialReceve:
            new_filename = filename + "_sorted"
            f = open(filename, 'w')
            for filename in files:
                f_node = open(filename, 'r')
                for entry in f_node:
                    f.write(entry)


        for conn in self.conn:
            conn.close()
        self.sock.close()

    def init_variables(self, filename, sorters):

        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        udp_sock.bind(("", DATA_PORT))
        self.udp_sock = udp_sock
        
        self.filename = filename
        self.sorters = sorters
        self.partition_dict = {}
        self.nodes = []
        self.addr_map = {}
        self.watchdog_condition = Condition()
        self.data_from = -1
        self.active_conns = []

    def beacon(self):
        logging.info("Beacon started")
        message = MAGIC.encode()
        try:
            while True:
                self.udp_sock.sendto(message, ('<broadcast>', BEACON_PORT))
                logging.debug("Beacon sent")
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logging.error(e)

    def connectionListener(self, requested_nodes):
        logging.info("Connection listener started")
        # create socket to wait for sorter nodes
        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.tcp_sock = tcp_sock
        # tcp_sock.setblocking(0)
        try:
            tcp_sock.bind((socket.gethostname(), DATA_PORT))
            # tcp_sock.bind(("", DATA_PORT))
        except OSError as e:
            logging.error(e)
            exit()
        tcp_sock.listen()
        tcp_sock.settimeout(10)
        # wait until nodes reply
        new_nodes = []
        while len(new_nodes) < requested_nodes:
            try:
                conn, addr = tcp_sock.accept()
            except socket.timeout:
                continue
            except KeyboardInterrupt:
                exit()
            except Exception as e:
                logging.error(e)
                exit()
            logging.info("Incoming connection from %s:%s" % (addr))
            data = conn.recv(len(MAGIC)+5).decode()
            # verify sorter is connecting
            if MAGIC != data[:len(MAGIC)] or (uniqueAddress and addr in [self.nodes[i]["addr"] for i in range(len(self.nodes))]):
                logging.info("Sorter node failed: %s:%s" % (addr))
                continue
            # conn.setblocking(0)
            # conn.settimeout(0)
            node = {}
            node["buffer"] = []
            node["b_buffer"] = b''
            node["state"] = 0
            node["time"] = datetime.now()
            node["data_sent"] = False
            node["conn"] = conn

            (ip, port_tcp) = addr
            port_udp = int(data[len(MAGIC):])
            self.addr_map[(ip, port_udp)] = node
            node["ip"] = ip
            node["port_udp"] = port_udp
            node["port_tcp"] = port_tcp
            if uniqueAddress:
                node["nodeID"] = ip
            else:
                node["nodeID"] = ip+':'+str(port_tcp)+':'+str(port_udp)
            node["udp_send_addr"] = (ip, port_udp)

            new_nodes.append(node)
            logging.info("Sorter node established: %s tcp:%s udp:%s" % (ip, port_tcp, port_udp))

        logging.info("Connection listener stopped")
        return new_nodes

    def processFile(self, chunk_size_unparsed='80M'):

        # calculate chunk size in bits
        chunk_size = utility.parseNumber(chunk_size_unparsed)
        # entry size = 100 bytes = 800 bits => entries_per_chunk = chunk_size/entry_size
        entries_per_chunk = chunk_size // 800
        
        f = open(self.filename, 'r')
        entries = []
        for entry in f:
            entries.append(entry)
            if len(entries) < entries_per_chunk:
                continue
            self.process_entries(entries)
            entries = []
        self.process_entries(entries)

        # flush buffers
        for i in range(self.sorters):
            if len(self.nodes[i]["buffer"]) > 0:
                self.flush_buffer(i)
                # finalize
            self.nodes[i]["conn"].sendall(utility.encodeData('FINISHED')+b'\0')
            # notify data is sent
            self.nodes[i]["data_sent"] = True
            self.send_message("sort", self.nodes[i])

    def listener(self):

        while True:
            data, addr = self.udp_sock.recvfrom(BufferSize)
            node = self.addr_map.get(addr)
            if node is None:
                logging.debug("Data from unknown source: %s:%s: %s" % (addr, data))
                continue
            d = utility.decodeData(data)
            m_type = d.get("type")
            if m_type is None:
                logging.debug("Message from %s:%s does not contain type" % (addr))
                continue
            
            nodeID = node["nodeID"]
            node["time"] = datetime.now()

            if m_type == "recv":
                pass

            if m_type == "state":
                state = int(d["state"])
                if node["state"] >= state: # older state, drop
                    logging.debug("Older state receive %s: %d-%s" % (nodeID, state, STATUS[state]))
                    continue
                logging.debug("State update %s: %d-%s" % (nodeID, state, STATUS[state]))
                node["state"] = state
                udp_send_addr = (node["ip"], node["port_udp"])

                if state == 2 and node["data_sent"]: # data
                    self.send_message("sort", node)
                    continue

                if state == 4: # done
                    if sequentialReceve and self.nodes.index(node) != self.data_from+1:
                        continue
                    self.send_message("send", node)
                    continue

                if state == 6: # finished
                    self.data_from += 1
                    self.send_message("close", node)
                    continue

                if state == 7: # close
                    node["conn"].close()
                    logging.info("Connection closed: %s" % (nodeID))
                    continue
    
    def data_receiver(self):
        self.active_conns = [self.nodes[i]["conn"] for i in range(len(self.nodes))]
        while True:
            readable, writable, errored = select.select(self.active_conns, [], [])
            for conn in readable:
                i = [i for i in range(len(self.nodes)) if self.nodes[i]["conn"] is conn][0]
                # node = (node for node in self.nodes if conn is node["conn"])[0]
                node = self.nodes[i]
                buffer = b''
                while True:
                    try:
                        buffer = conn.recv(BufferSize)
                    except BlockingIOError as e:
                        raise e
                    except ConnectionResetError as e:
                        raise e
                    except Exception as e:
                        raise e
                    if b'\0' not in buffer:
                        node["b_buffer"] += buffer
                        continue
                    b_data, ignored, node["b_buffer"] = (node["b_buffer"]+buffer).partition(b'\0')
                    data = utility.decodeData(b_data)
                    if data == "FINISHED":
                        break
                    for entry in data:
                        node["file"].write(entry)

    def send_message(self, message, node):
        data = {"type": message}
        self.udp_sock.sendto(utility.encodeData(data), node["udp_send_addr"])
        logging.debug("Sending '%s' to %s" % (message, node["nodeID"]))

    def watchdog(self):
        while True:
            self.watchdog_condition.wait()
            for i in range(self.nodes):
                node = self.nodes[i]

                if timediff <= NODE_KEEPALIVE_SOFT_TIMEOUT_SEC:
                    continue
                # soft timeout - request state update
                logging.debug("Node %s exceeded soft timeout" % (node[i]["nodeID"]))
                self.send_message("status", node[i])

                if timediff <= NODE_KEEPALIVE_HARD_TIMEOUT_SEC:
                    continue
                # TODO handle hard timeout

    def print_status_text_loop(self):
        while True:
            text = self.get_status_text()
            logging.info(text)
            time.sleep(REFRESH_TIME)

    def get_status_text(self):
        text = ''
        wakeup_watchdog = False
        for i in range(self.sorters):
            nodeID = self.nodes[i]["nodeID"]
            status = STATUS[self.nodes[i]["state"]]
            timediff = utility.getTimeDiff(self.nodes[i]["time"])


            if timediff >= NODE_KEEPALIVE_HARD_TIMEOUT_SEC:
                status = '(timeout)'

            if timediff >= NODE_KEEPALIVE_SOFT_TIMEOUT_SEC:
                wakeup_watchdog = True

            text += "-- %s: %9s " % (nodeID, status)

        text += " --"
        if wakeup_watchdog:
            self.watchdog_condition.notify_all()
        return text

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
    def process_entries(self, entries):
        for entry in entries:
            i = self.partition_dict[entry[0]]
            self.nodes[i]["buffer"].append(entry)
            if len(self.nodes[i]["buffer"]) >= LIMIT:
                self.flush_buffer(i)

    # sends buffer of node i to that node
    def flush_buffer(self, i):
        # d = {"type": "data", "entries" : self.nodes[i]["buffer"]}
        d = self.nodes[i]["buffer"]
        logging.debug("Sending %d entries" % (len(self.nodes[i]["buffer"])))
        self.nodes[i]["conn"].sendall(utility.encodeData(d)+b'\0')
        self.nodes[i]["buffer"] = []


class Sorter:

    def __init__(self):
        logging.info("Starting Sorter Node")

        while True:
            self.await_beacon()
            self.f = open("coordinator_" + self.coord[0], 'w+')
            Thread(target=self.receiver).start()
            self.dispatcher()
            logging.info("------------------------------------------")

    def await_beacon(self):

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        # sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(("", BEACON_PORT))
        # receive beacon from Sorter Node -- UDP!!
        while True:
            data, addr = sock.recvfrom(BufferSize)
            magic_bytes = data.decode()
            (ip, port) = addr
            if magic_bytes != MAGIC:
                return
            logging.info("Reader Node discovered on %s" % (ip))
            if self.start_connection(ip):
                sock.close()
                break
        logging.info("Connection established")

    def start_connection(self, ip):
        # create socket to connect to coordinator
        sock_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_tcp.setblocking(1)
        sock_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock_udp.bind(("", 0))
        try:
            sock_tcp.connect((ip, DATA_PORT))
        except Exception as e:
            logging.debug(e)
            sock_tcp.close()
            sock_udp.close()
            return False
        # send magic_bytes to coordinator, to verify
        (host, port) = sock_udp.getsockname()
        message = (MAGIC+str(port)).encode()
        sock_tcp.sendall(message)
        self.sock_tcp = sock_tcp
        self.sock_udp = sock_udp
        self.coord = (ip, DATA_PORT)
        # sock_udp.connect(self.coord)
        return True

    def dispatcher(self):
        self.setState(1)
        while True:
            try:
                data, addr = self.sock_udp.recvfrom(BufferSize)
            except BlockingIOError as e:
                logging.debug(e)
                continue
            except Exception as e:
                logging.error(e)
                self.sock_udp.close()
                exit()

            if addr != self.coord:
                logging.debug("Data from unknown source: %s:%s: %s" % (addr, data))
                continue
            d = utility.decodeData(data)
            m_type = d.get("type")
            if m_type is None:
                logging.debug("Message does not contain type" % (addr))
                continue
            logging.debug("Message type '%s' received", m_type)

            if m_type == "sort":
                if self.state != 2:
                    logging.debug("state: %d, 2 expected" % (self.state))
                    continue

                logging.info("Sort command received")
                self.setState(3)

                # Start sorting process
                Thread(target=self.sorter).start()

            if m_type == "status":
                logging.debug("Status command received")
                self.stateUpdate()

            if m_type == "send":
                if self.state != 4:
                    logging.debug("state: %d, 4 expected" % (self.state))
                    continue

                logging.info("Send command received")
                self.setState(5)
                Thread(target=self.sender).start()

            if m_type == "close":
                logging.info("Close command received")
                self.setState(7)
                self.sock_tcp.close()
                self.state = 0
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
        buffer = []
        for entry in self.sorted_data:
            buffer.append(entry)
            if len(buffer) >= LIMIT:
                logging.debug("Sending %d entries" % (len(buffer)))
                self.sock_tcp.sendall(utility.encodeData(buffer)+b'\0')
                buffer = []
        self.sock_tcp.sendall(utility.encodeData("FINISHED")+b'\0')
        logging.info("Sending finished")
        self.setState(6)

    def receiver(self):
        b_buffer = b''
        while True:
            try:
                buffer = self.sock_tcp.recv(BufferSize)
            except BlockingIOError as e:
                raise e
            except ConnectionResetError as e:
                raise e
            except Exception as e:
                raise e
            if b'\0' not in buffer:
                b_buffer += buffer
                continue
            b_data, ignored, b_buffer = (b_buffer+buffer).partition(b'\0')
            data = utility.decodeData(b_data)
            if data == "FINISHED":
                break
            for entry in data:
                self.f.write(entry)
        self.setState(2)

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