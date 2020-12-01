import socket
import select
import sys
import logging
from threading import Thread
from multiprocessing import Process
import numpy as np
import time
from datetime import datetime

import utility

BufferSize = utility.parseNumber("64K")
MAGIC = "distributed_sort"
BEACON_PORT = 11337 # UDP
CONNECTION_PORT = 11337 # TCP
LIMIT = 500
NODE_KEEPALIVE_SOFT_TIMEOUT_SEC = 5.0
NODE_KEEPALIVE_HARD_TIMEOUT_SEC = 15.0
# logging.basicConfig(format='%(levelname)s:%(message)s', level = logging.INFO)
logging.basicConfig(format='%(levelname)s:%(message)s', level = logging.DEBUG)
# unknown c:ready s:data c:busy c:done c:sending c:finished s:close
# 0       1       2      3      4      5         6          7
STATUS = ["unknown", "ready", "data", "busy", "done", "sending", "finished", "close"]
class Reader:

    def __init__(self, filename, min_nodes):
        logging.info("Starting Reader Node")

        self.init_variables(filename)

        beacon_thread = Process(target=self.beacon)
        beacon_thread.start()
        self.connectionListener(min_nodes, beacon_thread)
        beacon_thread.kill()
        logging.info("Beacon stopped")
        # listener_thread = Thread(target=self.listener)
        # listener_thread.start()

        self.create_partition_dict(len(self.nodes))
        # self.processFile()
        # listener_thread.join()

        Thread(target=self.processFile).start()
        try:
            self.listener()
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logging.error(e)

        for conn in self.conn:
            conn.close()
        self.sock.close()

    def init_variables(self, filename):

        self.filename = filename
        self.partition_dict = {}

        self.nodes = []
        self.conn = []
        self.buffers = []
        self.b_buffers = []
        self.state = []
        self.times = []
        self.data_sent = []

    def beacon(self):

        logging.info("Beacon started")
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        # enable broadcast
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        message = MAGIC.encode()
        try:
            while True:
                sock.sendto(message, ('<broadcast>', BEACON_PORT))
                logging.debug("Beacon sent")
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logging.error(e)

    def connectionListener(self, min_nodes, beacon_thread):
        logging.info("Connection listener started")
        # create socket to wait for sorter nodes
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.sock = sock
        # sock.setblocking(0)
        try:
            sock.bind((socket.gethostname(), CONNECTION_PORT))
        except OSError as e:
            logging.error(e)
            # beacon_thread.kill()
            exit()
        sock.listen()
        sock.settimeout(10)
        # wait until nodes reply
        while len(self.nodes) < min_nodes:
            try:
                conn, addr = sock.accept()
            except socket.timeout:
                continue
            except KeyboardInterrupt:
                # beacon_thread.kill()
                exit()
            except Exception as e:
                logging.error(e)
                # beacon_thread.kill()
                exit()
            (ip, port) = addr
            logging.info("Incoming connection from %s:" % (ip))
            data = conn.recv(len(MAGIC)).decode()
            # verify sorter is connecting
            if MAGIC != data or ip in self.nodes:
                conn.close()
                continue
            conn.setblocking(0)
            self.nodes.append(ip)
            self.conn.append(conn)
            self.buffers.append([])
            self.b_buffers.append(b'')
            self.state.append(0)
            self.times.append(datetime.now())
            self.data_sent.append(False)
            logging.info("Sorter node established: %s" % (ip))
        logging.info("Connection listener stopped")


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

        d = {"type": "sort"}
        # flush buffers
        for i in range(len(self.nodes)):
            if len(self.buffers[i]) > 0:
                self.flush_buffer(i)
            self.data_sent[i] = True
            self.conn[i].sendall(utility.encodeData(d))
            logging.debug("Sending 'sort' to %s" % (self.nodes[i]))

    def listener(self):

        f = open(self.filename+'_sorted', 'w')
        data_from = -1
        remaining_nodes = self.conn[:]
        reply_to = []
        while len(remaining_nodes) != 0:
            logging.debug(remaining_nodes)
            readable, writable, errored = select.select(remaining_nodes, reply_to, [], NODE_KEEPALIVE_SOFT_TIMEOUT_SEC/2)
            logging.debug("select %d %d %d" % (len(readable), len(writable), len(errored)))
            for conn in readable:
                i = self.conn.index(conn)
                node = self.nodes[i]
                buffer = b''
                while True:
                    try:
                        buffer = conn.recv(BufferSize)
                    except BlockingIOError as e:
                        # logging.debug(e)
                        break
                    except Exception as e:
                        logging.error(e)
                        exit()
                    if b'\0' not in buffer:
                        self.b_buffers[i] += buffer
                        continue
                    data, ignored, self.b_buffers[i] = (self.b_buffers[i]+buffer).partition(b'\0')
                    d = utility.decodeData(data)
                    self.times[i] = datetime.now()

                    m_type = d["type"]
                    if m_type == "state":
                        state = int(d["state"])
                        logging.debug("State receive %s: %d-%s" % (node, state, STATUS[state]))
                        if self.state[i] < state:
                            logging.debug("State update %s: %d-%s" % (node, state, STATUS[state]))
                            self.state[i] = state
                            if conn not in reply_to:
                                reply_to.append(conn)

                    if m_type == "data":
                        logging.debug("Data received")
                        if i == data_from+1:
                            data = d["data"]
                            logging.debug("Data accepted: %d entries" % (len(data)))
                            f.writelines(data)

            text = ''
            for i in range(len(self.nodes)):
                text += "-- %s: %8s" % (self.nodes[i], STATUS[self.state[i]])
                timediff = utility.getTimeDiff(self.times[i])
                conn = self.conn[i]
                if timediff <= NODE_KEEPALIVE_SOFT_TIMEOUT_SEC or conn not in remaining_nodes:
                    continue
                logging.debug("Node %s exceeded soft timeout" % (self.nodes[i]))
                if conn not in reply_to:
                    reply_to.append(conn)
                if timediff <= NODE_KEEPALIVE_HARD_TIMEOUT_SEC:
                    continue
                text += "(timeout)"
                logging.warn("Node %s exceeded hard timeout" % (self.nodes[i]))
            text += " --"
            logging.info(text)

# unknown c:ready s:data c:busy c:done c:sending c:finished s:close
# 0       1       2      3      4      5         6          7

            for conn in writable:
                reply_to.remove(conn)
                i = self.conn.index(conn)
                node = self.nodes[i]
                state = self.state[i]

                if state == 2 and self.data_sent[i]: # sort
                    d = {"type": "sort"}
                    conn.sendall(utility.encodeData(d))
                    logging.debug("Sending 'sort' to %s" % (node))
                    continue

                if state == 4: # send
                    if i != data_from+1:
                        continue
                    d_send = {"type": "send"}
                    conn.sendall(utility.encodeData(d_send))
                    logging.debug("Sending 'send' to %s" % (node))
                    continue

                if state == 6: # close
                    data_from += 1
                    d_send = {"type": "close"}
                    conn.sendall(utility.encodeData(d_send))
                    logging.debug("Sending 'close' to %s" % (node))
                    continue

                if state == 7: # finish
                    conn.close()
                    remaining_nodes.remove(conn)
                    logging.info("Connection closed: %s" % (node))
                    continue

                if timediff <= NODE_KEEPALIVE_SOFT_TIMEOUT_SEC:
                    continue
                d_send = {"type": "status"}
                conn.sendall(utility.encodeData(d_send))
                logging.debug("Sending 'status' to %s" % (node))

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
            part = self.partition_dict[entry[0]]
            self.buffers[part].append(entry)
            if len(self.buffers[part]) >= LIMIT:
                self.flush_buffer(part)

    def flush_buffer(self, i):
        node = self.conn[i]
        d = {"type": "data", "entries" : self.buffers[i]}
        logging.debug("Sending %d entries" % (len(self.buffers[i])))
        node.sendall(utility.encodeData(d))
        self.buffers[i] = []


class Sorter:

    def __init__(self):
        logging.info("Starting Sorter Node")

        while True:
            self.await_beacon()
            self.f = open("coordinator_" + self.coord_ip, 'w+')
            self.dispatcher()

    def await_beacon(self):

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("", BEACON_PORT))
        # receive beacon from Sorter Node -- UDP!!
        while True:
            data, addr = sock.recvfrom(len(MAGIC))
            # data format: #magic_bytes-#memory_avaible(rest)
            magic_bytes = data.decode()
            (ip, port) = addr
            if magic_bytes != MAGIC:
                return
            logging.info("Reader Node discovered on %s" % (ip))
            if self.start_connection(ip):
                # sock.close()
                break

    def start_connection(self, ip):
        # create socket to connect to coordinator
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(1)
        try:
            sock.connect((ip, CONNECTION_PORT))
        except Exception as e:
            logging.debug(e)
            return False
        # send magic_bytes to coordinator, to verify
        message = MAGIC.encode()
        sock.sendall(message)
        self.conn = sock
        self.coord_ip = ip
        return True

# unknown c:ready s:data c:busy c:done c:sending c:finished s:close
# 0       1       2      3      4      5         6          7
    def dispatcher(self):
        self.setState(1)
        buffer = b''
        while True:
            while b'\0' not in buffer:
                try:
                    buffer += self.conn.recv(BufferSize)
                except BlockingIOError as e:
                    logging.debug(e)
                    continue
                except Exception as e:
                    logging.error(e)
                    self.conn.close()
                    exit()
            
            data, ignored, buffer = buffer.partition(b'\0')
            d = utility.decodeData(data)
            m_type = d["type"]
            logging.debug("Message type '%s' received", m_type)

            if m_type == "data":
                if self.state > 2:
                    logging.debug("state: %d, <=2 expected" % (self.state))
                    continue

                logging.debug("Data received")
                if self.state != 2:
                    self.setState(2)

                entries = d["entries"]
                logging.info("Data received: %d entries" % (len(entries)))
                for entry in entries:
                    self.f.write(entry)

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
                time.sleep(5)
                self.conn.close()
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
        d = {"type": "data", "state":self.state}
        for entry in self.sorted_data:
            buffer.append(entry)
            if len(buffer) >= LIMIT:
                d["data"] = buffer
                logging.debug("Sending %d entries" % (len(buffer)))
                self.conn.sendall(utility.encodeData(d))
                buffer = []
        logging.info("Sending started")
        self.setState(6)

    def setState(self, state):
        self.state = state
        self.stateUpdate()

    def stateUpdate(self):
        d = {"type": "state", "state" : self.state}
        logging.debug("Sending state update %d-%s" % (self.state, STATUS[self.state]))
        self.conn.sendall(utility.encodeData(d))

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
