#!/usr/bin/env python3

import pickle
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

    def __init__(self, host=None, port=None):
        self.host = host
        self.port = port

    def _process_subscriber(self, addr, data):
        """Code for processing subscriber inputs (adding to topic queue)"""
        assert addr in self.network_store
        new_topics = pickle.loads(data)
        self.network_store[addr][1] = self.network_store[addr][1] + new_topics

    def _process_publisher(self):
        """Code for processing publisher messages, streaming input"""
        return NotImplementedError

    def receive_connections(self):
        """
            Broker receives a single connection from client right now
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))

            # Up to queue_length requests in queue
            s.listen(self.queue_length)
            conn, addr = s.accept()

            with conn:
                print('Connected by', addr)
                while True:
                    data = conn.recv(1024)

                    print(f"----data: {data}----") # <-- debugging information here

                    # Works because of lazy evaluation. More robust approach?
                    if not data:
                        break
                    if addr in self.network_store:
                        client_type = self.network_store[addr][0]
                        if client_type == "publisher":
                            self._process_publisher()
                        elif client_type == "subscriber":
                            self._process_subscriber(addr, data)
                    elif data.decode('utf-8') == 'Subscriber':
                        assert addr not in self.network_store
                        self.network_store[addr] = ["subscriber", []]
                    elif data.decode('utf-8') == 'Publisher':
                        assert addr not in self.network_store
                        self.network_store[addr] = ["publisher", []]

                    print(f"self.network_store: {self.network_store}")
                    conn.sendall(b"Broker finished processing")


    @classmethod
    def set_message_queue_len(cls, length):
        """Changing the default queue length for messages"""
        cls.queue_length = length
