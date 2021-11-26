import os, sys
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

import socket
from sqlInterpreter import SqlInterpreter

from db.database import Database
import time

HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 15043        # Port to listen on (non-privileged ports are > 1023)

while True:
    try:
        print('ye')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.settimeout(5)
            s.bind((HOST, PORT))
            s.listen()
            conn, addr = s.accept()
            inter = SqlInterpreter(Database)
            with conn:
                print('Connected by', addr)
                while True:
                    try:
                        data = conn.recv(1024)
                        data = data.decode('utf-8')
                        #send_query(data)
                        a = str(inter.interpret(data))
                        if data == 'exit':
                            print('done')
                            break
                        #conn.sendall(f'Sending back -> {data}'.encode())
                        conn.sendall(f'{a}'.encode())
                    except Exception as e:
                        print(e)
                        print('bye')
                        break
    # except timeout               
    except (KeyboardInterrupt, socket.timeout):
        # print('ki')
        break
    # time.sleep(1)
            #conn.sendall(f'Sending back -> '+data)
