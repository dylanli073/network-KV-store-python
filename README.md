# network-KV-store-python
This is a basic message broker system that will allow publishers and subscribers to communicate with one another. 

### Running the code
Run the code by running ```python3 broker.py```, before running ```python3 subscriber.py```. The code will ask you to input the topics that the subscriber wishes to subscribe to. In the full implementation, there will also be a publisher running that will intermittently publish to the broker. The broker will index into a dictionary of addresses that it is currently connected to and publish the message to the relevant subscribers.