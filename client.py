import socket
import re

# Server's IP(local).
HOST = "127.0.0.1"
# Server's Port.
PORT = 5050
# Socket creation.
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# Connect to the server.
client.connect((HOST, PORT))

# Regular expressions to filter and validate the user input.
query_pattern = '^\s*select\s+\*\s+from\s+_*[a-zA-Z]+[a-zA-Z0-9_]*\s*$'
reg = re.compile(query_pattern, flags = re.IGNORECASE)
exit_pattern = '^\s*exit\s*$'
reg2 = re.compile(exit_pattern, flags = re.IGNORECASE)
reserved_keywords_pattern = '^select|from$'
reg3 = re.compile(reserved_keywords_pattern, flags = re.IGNORECASE)

def transform(qr):
    '''
    Lowercase the 'select' and 'from' keywords of the query and replace all the whitespace characters with a single space.
    Also remove any leading or trailing whitespaces.
    '''
    # Split the query in two parts, the first is the query before the 'from' and second is the query after it.
    spl = qr.split('from', 1)
    # Lowercase the reserved keywords 'select' and 'from' and then and concatenate the query back to one piece.
    tolower = spl[0].lower() + 'from' + spl[1]
    # Remove any leading or trailing whitespaces.
    final = re.sub('\s+', ' ', tolower).strip()
    return final


def checkTableName(str):
    '''
    Check that the table's name is not the reserved keyword 'select' or 'from'.
    '''
    # Split the query into it's words and return a match object if the table's name isn't 'select' or 'from', else return null.
    t = str.split(' ')
    return reg3.search(t[3], 0)


while True:
    print('-------------------------------------------------------')
    print('Enter your \'select\' query or type \'exit\' to disconnect:\n')
    q = input()
    # If the user input is a valid 'select * from table_name' query.
    if reg.search(q, 0):
        query = transform(q)
        # Check if the match object is not null(the table's name is 'select' or 'from').
        if checkTableName(query):
            print('\nYou can\'t use this name for a table.\n\n')
        # If the table's name is not 'select' or 'from'.
        else:
            # Send the query to the server and check if it is sent successfully.
            # If it isn't sent successfully it means the socket has closed.
            if client.sendall(str(query + chr(6)).encode("utf-8")) is not None:
                print('\nError: Lost connection to the server.')
                # Close the connection and exit.
                client.close()
                break
            r = ""
            br = False
            # If the query is sent successfully start receiving the result.
            while True:
                temp = client.recv(512).decode("utf-8")
                # If we received > 0 bytes.
                if temp:
                    r += temp
                    # If we have received the entire query the last character is a non-printable character
                    # that indicates the end of the message.
                    if r[-1] == chr(6):
                        break
                # If we received 0 bytes the socket has closed.
                else:
                    print('\nError: Lost connection to the server.')
                    # Close the connection and exit.
                    client.close()
                    br = True
            if br:
                break
            # Print the result.
            r = r[:-1]
            print(r)
    # If the user input is not a valid 'select * from table_name' query.
    else:
        # If the user input is the word 'exit'.
        if reg2.search(q, 0):
            # Send the 'exit' to the server and check if it is sent successfully.
            # If it isn't sent successfully it means the socket has closed.
            if client.sendall(str(q + chr(6)).encode("utf-8")) is not None:
                print('\nError: Lost connection to the server.')
            else:
                print('\nDisconnected from the server.')
            # Close the connection and exit.
            client.close()
            break
        # If the user input is not a valid 'select * from table_name' query or 'exit'.
        else:
            print('\nUnrecognizable input.\n\n')
