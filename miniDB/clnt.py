#!/usr/bin/env python3

import socket
import sys
import readline ## this is for repl history dont delete pls thanks

HOST = '127.0.0.1'  # The server's hostname or IP address
PORT = 15043        # The port used by the server

while 1:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            print(f'Connected to {(HOST, PORT)}')
            try:
                while 1:
                    msg = input()
                    s.sendall(msg.encode())
                    data = s.recv(1024)

                    print('Received:', data.decode('utf-8'))
                    if msg=='exit':
                        break

            except KeyboardInterrupt:
                print('Interrupted')
                s.close()
                break
            
    except (ConnectionResetError, BrokenPipeError):
        print('lost connection')
    except ConnectionRefusedError:
        print('cant connect, try again')
        # break
    # except e

    inp = input('enter to reconnect, c to close\n')
    if inp == 'c':
        sys.exit()
