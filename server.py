import socket
import threading
from database import Database
import pickle

#server's network configuration
host = "127.0.0.1"
port = 12345

#initialize server's socket#

#server's socket is an IPv4 socket with TCP stream
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#bind network's configuration with the created socket
server.bind((host, port))
#set server's socket in listening mode
server.listen(5)

#print server's status
print("[*] Server is listening on %s:%d..." % (host, port))

db = Database("smdb", load=True)

#function to handle a client
def handle_client(client_socket):
    #client's query
    client_query = client_socket.recv(1024).decode()
    db_result = db.select(client_query.split(" ")[3], client_query.split(" ")[1], return_object=True)
    pickle_obj = pickle.dumps(db_result)
    client.send(pickle_obj)
    #print(pickle_obj)
    client_socket.close()
    
# SELECT * FROM table_name
        
while True:
    #accept client's connection
    client, addr = server.accept()

    #welcome message to client and request a query
    client.send("[*] Hello please insert a query".encode())

    #print client's socket info
    print("Accepted connection from -> %s:%d" % (addr[0],addr[1]))
    
    #create a thread for the client and call handle_client function to handle the client
    client_handler = threading.Thread(target= handle_client(client), args= (client, ))
    
    #start client's thread
    client_handler.start()