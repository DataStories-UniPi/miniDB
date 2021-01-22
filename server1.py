import socket
import threading
from tabulate import tabulate
from database import Database

def show_to_string(self):
    '''
    Return the table as a string in a preety form.
    '''
    result = f"\n## {self._name} ##\n"
    # headers -> "column name (column type)"
    headers = [f"{col} ({tp.__name__})" for col, tp in zip(self.column_names, self.column_types)]
    if self.pk_idx is not None:
        # table has a primary key, add PK next to the appropriate column
        headers[self.pk_idx] = headers[self.pk_idx] + " #PK#"
    # detect the rows that are not full of nones (these rows have been deleted)
    # if we dont skip these rows, the returning table has empty rows at the deleted positions
    non_none_rows = [row for row in self.data if any(row)]
    # print using tabulate
    result += tabulate(non_none_rows[:], headers = headers) + "\n\n"
    return result

def handle_query(query):
    '''
    Handle the client's query. This function can be extended to fully support SQL statements.
    Now it only supports 'select * from table_name' statements.
    '''
    # Extract table name from query.
    table_name = (query[query.find("from") + len("from"):]).strip()
    # If it exists, send the table in a preety form, else send as a result that the table doesn't exist.
    if table_name in db.tables:
        result = show_to_string(db.tables[table_name])
    else:
        result = f"\nTable '{table_name}' doesn't exist.\n\n"
    return result + chr(6)

def client(connection, address):
    '''
    The server's main functionality. For every new client a new thread is created and
    executes this function.
    '''
    while True:
        query = ""
        while True:
            # Receive the client's query.
            received = connection.recv(512).decode("utf-8")
            # If we received > 0 bytes.
            if received:
                query += received
                # If we have received the entire query the last character is a non-printable character
                # that indicates the end of the message.
                if query[-1] == chr(6):
                    break
            # If we received 0 bytes the socket has closed.
            else:
                print(f"\n[Server] Error: Lost connection to |{address[0]} : {address[1]}|.")
                print(f"\n[Server] Active clients: {threading.activeCount() - 2}.")
                connection.close()
                return
        # Remove the non-printable character from the query.
        query = query[:-1]
        print(f"\n[Server] Received the query '{query}' from |{address[0]} : {address[1]}|.")
        # The terminating condition for the connection is: query = "exit".
        if query == "exit":
            print(f"\n[Server] Disconnected from |{address[0]} : {address[1]}|.")
            print(f"\n[Server] Active clients: {threading.activeCount() - 2}.")
            connection.close()
            return
        else:
            # Return the query's result and send it to the client.
            result = handle_query(query)
            # Sending the result back to the client.
            sent_result = connection.sendall(str(result).encode("utf-8"))
            # If the result is not sent successfully.
            if sent_result is not None:
                print(f"\n[Server] Error: Lost connection to |{address[0]} : {address[1]}|.")
                connection.close()
                return
            else:
                print(f"\n[Server] Sent the result for the query '{query}' to |{address[0]} : {address[1]}|.")


# Database to load.
db = Database("vsmdb", load = True)
# Port number.
PORT = 5050
# Socket creation.
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# Bind server to IP and Port.
server.bind(("", PORT))
# Start listening for requests.
server.listen(5)
print("\n[Server] Server started.")
print("\n[Server] Active clients: 0.")
try:
    while True:
        # Accept a request.
        connection, address = server.accept()
        # Create a new thread that will execute the 'client' function.
        thread = threading.Thread(target = client, args = (connection, address))
        print(f"\n[Server] Connected to |{address[0]} : {address[1]}|.")
        print(f"\n[Server] Active clients: {threading.activeCount()}.")
        # Start the new thread.
        thread.start()
except:
    server.close()
    print("\n[Server] Server stopped.")
