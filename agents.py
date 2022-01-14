#!/usr/bin/env python3

import logging
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
                                "Broker finished processing topic list")

            print("Messages from subscribed topics from broker: ")

            while True:
                # later change this recv --> select to process stdin
                data = s.recv(1024)
                print(f"----message from broker: {data.decode('utf-8')}----")


class Publisher:
    """Publisher implementation in the pub-sub system"""
    topic = None
    last_message = None

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

            # Collecting topics and sending them in byte rep to broker
            topic = input("Enter publisher topic: ")
            assert topic != ""
            self.topic = topic.strip()

            s.sendall(f'Publisher-{self.topic}'.encode('utf-8'))
            self.last_message = s.recv(1024)

            print(f"Last received message: {self.last_message}")

            while True:
                # later change this recv --> select to process stdin
                message = input("Enter message to broadcast: ")
                assert message != ""
                s.sendall(message.encode('utf-8'))


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

    def _publish_to_subscribers(self, topic_fds, topic, message):
        """Iterate through and send msg to subscribers"""
        for fd in topic_fds:
            conn = self.network_store[fd][1]
            conn.sendall(topic.encode('utf-8') + b": " + message)

    def _process_subscriber(self, fd, data):
        """Code for processing subscriber inputs (adding to topic queue)"""
        assert fd in self.network_store
        new_topics = pickle.loads(data)
        self.network_store[fd][3] = self.network_store[fd][3] + new_topics
        for topic in new_topics:
            try:
                self.topic_subscribers[topic].add(fd)
            except KeyError:
                self.topic_subscribers[topic] = {fd}

    def _not_dumb(self, data):
        """Returns true if data is dumb"""
        if data == "dumb":
            return False
        return True

    def _process_publisher(self, fd, message):
        """Code for processing publisher messages, streaming input. Assume
        that publishers are associated only with a single topic. Data
        should be unpickled before passing into the function.
        """
        assert fd in self.network_store
        assert self.network_store[fd][0] == "publisher"

        # first, do a sanity check on the data payload
        assert len(message) < 1024
        assert self._not_dumb(message)

        # second, index into the self.network_store dictionary to identify
        # the topic associated with the publisher. Only supporting 1 topic per pub
        topic = self.network_store[fd][3]

        if topic in self.topic_subscribers:
            subscriber_fds = self.topic_subscribers[topic]
            self._publish_to_subscribers(subscriber_fds, topic, message)

    def _close_socket_conn(self, conn):
        self.fd_read_waits.remove(conn.fileno())
        try:
            self.network_store.pop(conn.fileno())
        except KeyError:
            pass
        finally:
            conn.close()

    def receive_connections(self):
        """
            Broker receives connections from subs and pubs and separately handles
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))

            # Up to queue_length requests in queue
            s.listen(self.queue_length)

            self.master_socket = s.fileno()
            self.fd_read_waits.append(self.master_socket)
            print(f"s.fileno() (ie. master file no): {self.master_socket}")
            assert s.fileno() > -1

            while True:
                read_socket, _, _ = select.select(self.fd_read_waits,
                                            self.fd_write_waits,
                                            self.fd_xlist_waits)
                read_fd = read_socket[0]

                if read_fd == self.master_socket:
                    conn, addr = s.accept()
                    self.fd_read_waits.append(conn.fileno())
                    print(f"conn.fileno: {conn.fileno()}")

                    print(f"New socket conn to addr: {addr} @ fn: {conn.fileno()}")

                    data = conn.recv(1024).decode('utf-8')
                    publisher_data = data.split("-")
                    if data == 'Subscriber':
                        assert conn.fileno() not in self.network_store
                        self.network_store[conn.fileno()] = ["subscriber",
                                                            conn, addr, []]
                        conn.sendall(b"Welcome to EverestMQ, subscriber!")
                    elif publisher_data[0] == 'Publisher':
                        assert conn.fileno() not in self.network_store
                        self.network_store[conn.fileno()] = ["publisher",
                                                conn, addr, publisher_data[1]]
                        conn.sendall(b"Welcome to EverestMQ, publisher!")
                    else:
                        logging.error("Incorrect connection format")
                        self._close_socket_conn(conn)
                elif read_fd in self.network_store:
                    conn = self.network_store[read_fd][1]
                    addr = self.network_store[read_fd][2]

                    print(f"Connected by {addr}")

                    data = conn.recv(1024)
                    if not data:
                        logging.error("Connection closed")
                        self._close_socket_conn(conn)
                    else:
                        client_type = self.network_store[read_fd][0]
                        if client_type == "publisher":
                            self._process_publisher(read_fd, data)
                        elif client_type == "subscriber":
                            self._process_subscriber(read_fd, data)
                            conn.sendall(b"Broker finished processing topic list")
                else:
                    logging.error("Invalid fd, breaking all pipes")
                    for payload in self.network_store.values():
                        conn = payload[1]
                        self._close_socket_conn(conn)
                    raise KeyError

    @classmethod
    def set_message_queue_len(cls, length):
        """Changing the default queue length for messages"""
        cls.queue_length = length
