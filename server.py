import socket
import threading

#server's network configuration
host = "localhost"
port = 12345

#initialize server's socket#

#server's socket is an IPv4 socket with TCP stream
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#bind network's configuration with the created socket
server.bind((host, port))

#set server's socket in listening mode
server.listen(1)

#print server's status
print("[*] Server is listening on %s:%d..." % (host, port))

#function to handle a client
def handle_client(client_socket):
    #client's query
    client_query = client_socket.recv(1024)
    print(client_query.decode())
    client_socket.close()

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