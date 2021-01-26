import socket
import sys
import pickle
from table import Table

target_host = sys.argv[1]
target_port = 12345

#create a socket object.
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#connect the client
client.connect((target_host, target_port))


print(client.recv(2048).decode())
query = str(input("SQL query:"))
#send some data.
client.send(query.encode())

response = pickle.loads(client.recv(2048))
Table.show(response)

client.close()