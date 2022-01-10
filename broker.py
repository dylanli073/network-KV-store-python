#!/usr/bin/env python3

from agents import Broker, BROKER_IP, BROKER_PORT

def main():
    """Running multiple client connections from this driver code"""
    Broker.set_message_queue_len(4)
    print(Broker.queue_length)

    server = Broker(host=BROKER_IP, port=BROKER_PORT)
    server.receive_connections()


if __name__ == "__main__":
    main()
