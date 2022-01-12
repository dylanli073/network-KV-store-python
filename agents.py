#!/usr/bin/env python3

import pickle
import select
import socket

BROKER_IP = "127.0.0.1"
BROKER_PORT = 8088
N_TOPIC_LIMIT = 16

class Subscriber:
    """Subscriber implementation in the pub-sub system"""
    topics = []
    last_message = ""

    def __init__(self, host=None, port=None):
        self.host = host
        self.port = port

    def setup_connection(self):
        """Setting up TCP (self.SOCK_STREAM) connection with server
        Args:
            ip_address -> server's string hostname (string)
            port -> port used to process incoming connections
                on server (int)

        Does not support modifying topics after initialization atm
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            s.sendall(b'Subscriber')
            self.last_message = s.recv(1024)

            print(f"self.last_message: {self.last_message}")

            # Collecting topics and sending them in byte rep to broker
            for _ in range(N_TOPIC_LIMIT):
                topic = input("Enter additional topic (if end, write 'END'): ")
                assert topic != ""
                if topic == "END":
                    break
                self.topics.append(topic.strip())

            # Sending topics to broker
            s.sendall(pickle.dumps(self.topics))
            self.last_message = s.recv(1024)
            print(f"Last received message: {self.last_message}")
            assert(self.last_message.decode("utf-8") ==
                                "Broker finished processing")


class Broker:
    """Broker implementation in the pub-sub system"""
    queue_length = 3

    # Stores metadata about each connection
    network_store = dict()

    # Store the file descriptors we are waiting to read from
    fd_read_waits = []
    fd_write_waits = []
    fd_xlist_waits = []
    master_socket = None

    # storing fds corresponding to topics (key: topics, value: conn)
    topic_subscribers = dict()

    def __init__(self, host=None, port=None):
        self.host = host
        self.port = port

    def _publish_to_subscribers(self, topic, message):
        """Iterate through subscribers and send msg to subscribers"""


    def _process_subscriber(self, fd, data):
        """Code for processing subscriber inputs (adding to topic queue)"""
        assert fd in self.network_store
        new_topics = pickle.loads(data)
        self.network_store[fd][1] = self.network_store[fd][1] + new_topics

    def _process_publisher(self, fd, data):
        """Code for processing publisher messages, streaming input. Assume
        that publishers are associated only with a single topic
        """
        assert fd in self.network_store
        # first, do a sanity check on the data payload
        # second, index into the self.network_store dictionary to identify
        # the topic associated with the publisher
        # call self._publish_to_subscribers() to publish to the relevant subs
        return NotImplementedError

    def receive_connections(self):
        """
            Broker receives a single connection from client right now
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))

            # Up to queue_length requests in queue
            s.listen(self.queue_length)

            # <--> write while statement here
            # # address is the address bound to the socket on the other end of the conn
            # conn, addr = s.accept()
            # self.fd_read_waits.append(conn.fileno())
            self.master_socket = s.fileno()
            self.fd_read_waits.append(self.master_socket)
            assert s.fileno > -1

            while True:
                read_socket, _, _ = select.select(self.fd_read_waits,
                                            self.fd_write_waits,
                                            self.fd_xlist_waits)
                read_fd = read_socket[0]

                if read_fd == self.master_socket:
                    conn, addr = s.accept()
                    self.fd_read_waits.append(conn.fileno())
                    conn.connect(addr)

                    print(f"Master socket connected by {addr}")

                    data = conn.recv(1024)
                    if data.decode('utf-8') == 'Subscriber':
                        assert conn.fileno() not in self.network_store
                        self.network_store[conn.fileno()] = ["subscriber", conn, addr, []]
                    elif data.decode('utf-8') == 'Publisher':
                        assert conn.fileno() not in self.network_store
                        self.network_store[conn.fileno()] = ["publisher", conn, addr, []]
                elif read_fd in self.network_store:
                    conn = self.network_store[read_fd][1]
                    addr = self.network_store[read_fd][2]

                    print(f"Connected by {addr}")

                    data = conn.recv(1024)
                    if not data:
                        conn.close()
                        self.fd_read_waits.remove(conn.fileno())
                        self.network_store.pop(conn.fileno())
                    else:
                        client_type = self.network_store[read_fd][0]
                        if client_type == "publisher":
                            self._process_publisher(read_fd, data)
                        elif client_type == "subscriber":
                            self._process_subscriber(read_fd, data)
                else:
                    raise KeyError

                print(f"self.network_store: {self.network_store}")
                conn.sendall(b"Broker finished processing")


    @classmethod
    def set_message_queue_len(cls, length):
        """Changing the default queue length for messages"""
        cls.queue_length = length
