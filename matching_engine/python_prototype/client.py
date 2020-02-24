import socket
import json

# Create a socket (SOCK_STREAM means a TCP socket)
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    # Connect to server and send data
    sock.connect(("localhost", 8080))
    order = {'id': 0, 'type': 'limit', 'side': 'buy', 'price': 100, 'quantity': 322}
    serialized_order = json.dumps(order).encode('utf-8')
    sock.sendall(serialized_order)

    # Receive data from the server and shut down
    received = str(sock.recv(1024), "utf-8")
finally:
    sock.close()

print("Sent:     {}".format(order))
print("Received: {}".format(received))