#!/usr/bin/env python3

from agents import Publisher, BROKER_IP, BROKER_PORT

def main():
    """Running multiple client connections from this driver code"""
    client = Publisher(host=BROKER_IP, port=BROKER_PORT)

    client.setup_connection()


if __name__ == "__main__":
    main()
