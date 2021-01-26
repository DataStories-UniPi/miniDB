import socket
import sys
import pickle
from table import Table

#client's network configuration
target_host = sys.argv[1]
target_port = 12345

#initialize client's socket#
#create a socket object.
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    #connect the client socket
    client.connect((target_host, target_port))
    #receive welcome message from server
    print(client.recv(2048).decode())
    query = str(input("SQL query:"))
    #send some data.
    client.send(query.encode())
    #print query's result to the client
    response = pickle.loads(client.recv(2048))
    Table.show(response)
    #close the client socket
    client.close()
except Exception as e:
    print(str(e))