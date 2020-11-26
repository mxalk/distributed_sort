class Sorter:

    def __init__(ram_available):

        self.init_variables(ram_available)
        self.await_connection()
        # connection is now established, and data can be received by self.conn.recv(buffersize)

    def init_variables(ram_available):

        self.ram_available = ram_available

    def beacon():

        sock = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        # enable broadcast
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        message = MAGIC + '-' + self.ram_available
        while True:
            sock.sendto(message, ('<broadcast>', BEACON_PORT))
            logging.info("Beacon sent...")
            time.sleep(1)

    def await_connection():
        # create socket to listen for coordinator
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((socket.gethostname(), CONNECTION_PORT))
        sock.listen()
        # start beacon to find coordinator
        beacon_thread = threading.Thread(target=self.beacon)
        # wait until coordinator replies
        while True:
            conn, addr = sock.accept()
            logging.info("Incoming connection from %s" % (addr))
            data = conn.recv(BufferSize)
            # verify coordinator is connecting
            if MAGIC != data:
                conn.close()
                continue
            # kill beacon once connection is established
            beacon_thread.stop()
            self.conn = conn
            logging.info("Coordinator established, awaiting data...")
            break

    def radix_sort(arr):
        char_list = [chr(x) for x in range(32, 127)]
        max_length = 4  # compare first 4 characters

        for i in range(max_length):
            bucket = {}
            for char in char_list:
                bucket[ord(char)] = []
            for ele in arr:
                temp_word = tuple[0:4]
                bucket[ord(temp_word[max_length - i - 1])].append(ele)

            index = 0
            for char in char_list:
                if (len(bucket[ord(char)])) != 0:
                    for i in bucket[ord(char)]:
                        arr[index] = i
                        index = index + 1
