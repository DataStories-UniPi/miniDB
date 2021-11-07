import socket

import os, sys
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from db import database as database

import logging

HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 65432        # Port to listen on (non-privileged ports are > 1023)

db = database.Database('vsmdb_poc', load=False)
#try:
#    db.drop_table('classroom')
#except:
#    pass
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    conn, addr = s.accept()
    try:
        with conn:
            print('Connected by', addr)
            while True:
                data = conn.recv(1024)
                data = data.decode('utf-8')
                #send_query(data)
                #a = str(sqlInterpreter.main(query=data))

                out = 'Query "'+str(data)+'" not found.'
                if data == 'create_table':
                    # create a single table named "classroom"
                    out = db.create_table('classroom', ['building', 'room_number', 'capacity'], [str,str,int])
                elif data == 'insert_data':
                    # insert 5 rows
                    db.insert('classroom', ['Packard', '101', '500'])
                    db.insert('classroom', ['Painter', '514', '10'])
                    db.insert('classroom', ['Taylor', '3128', '70'])
                    db.insert('classroom', ['Watson', '100', '30'])
                    db.insert('classroom', ['Watson', '120', '50'])
                    out = 'Data inserted to table successfully!'
                elif data == "SELECT * FROM classroom":
                    out = db.select(table_name='classroom', columns='*')

                if data == 'exit()':
                    print('done')
                    break
                #conn.sendall(f'Sending back -> {data}'.encode())
                conn.sendall(f'{out}'.encode())
                #conn.sendall(f'Sending back -> '+data)
    except Exception as e:
        print(e)
        conn.close()