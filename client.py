import socket
import sys

target_host = sys.argv[1]
target_port = 12345

#create a socket object.
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#connect the client
client.connect((target_host, target_port))

query = input("SQL query: ")
#send some data.
client.send(query.encode())

#receive some data.
response = client.recv(1024)

print(response.decode())