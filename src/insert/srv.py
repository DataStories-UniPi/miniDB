import socket
import sqlInterpreter

HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 65432        # Port to listen on (non-privileged ports are > 1023)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    conn, addr = s.accept()
    with conn:
        print('Connected by', addr)
        while True:
            data = conn.recv(1024)
            data = data.decode('utf-8')
            #send_query(data)
            a = str(sqlInterpreter.main(query=data))
            if data == 'exit()':
                print('done')
                break
            #conn.sendall(f'Sending back -> {data}'.encode())
            conn.sendall(f'{a}'.encode())
            #conn.sendall(f'Sending back -> '+data)
