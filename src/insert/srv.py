import os, sys
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

import socket
from sqlInterpreter import SqlInterpreter

from db.database import Database

HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 65432        # Port to listen on (non-privileged ports are > 1023)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    conn, addr = s.accept()
    inter = SqlInterpreter(Database)
    with conn:
        print('Connected by', addr)
        while True:
            data = conn.recv(1024)
            data = data.decode('utf-8')
            #send_query(data)
            a = str(inter.interpret(data))
            if data == 'exit()':
                print('done')
                break
            #conn.sendall(f'Sending back -> {data}'.encode())
            conn.sendall(f'{a}'.encode())
            #conn.sendall(f'Sending back -> '+data)
