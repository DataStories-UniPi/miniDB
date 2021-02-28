from database import Database
import re
import traceback


class SqlInterpreter:
    def __init__(self, db=None):
        self.db = db
        print("Instantiated Sql Interpreter")

    def get_type(self, type):
        if type == "str":
            return str
        elif type == "int":
            return int
        elif type == "float":
            return float
        elif type == "complex":
            return complex
        elif type == "bool":
            return bool
        elif type == "bytes":
            return bytes
        elif type == "bytearray":
            return bytearray
        elif type == "memoryview":
            return memoryview
        else:
            return str

    def interpret(self, query):
        print()
        if re.search("DATABASE", query):
            self.database_query(query)
        elif re.search("TABLE|COPY|EXPORT", query):
            self.table_query(query)
        elif re.search("INDEX", query):
            self.index_query(query)
        elif re.search("INSERT|DELETE|UPDATE", query):
            self.insert_delete_update_query(query)
        elif re.search("SELECT", query):
            self.select_query(query)
        else:
            print("Unknown query " + query)

    def database_query(self, query):
        try:
            if re.search("CREATE", query):
                self.db = Database(query.split(" ")[2], load=False)
                print("CREATE Database query [ " + query + " ] was successful")
            elif re.search("DROP", query):
                self.db = Database(query.split(" ")[2], load=True)
                self.db.drop_db()
                self.db = None
                print("DROP Database query [ " + query + " ] was successful")
            elif re.search("LOAD", query):
                self.db = Database(query.split(" ")[2], load=True)
                print("LOAD Database query [ " + query + " ] was successful")
            elif re.search("SAVE", query):
                self.db = Database(query.split(" ")[2], load=True)
                self.db.save()
                print("SAVE Database query [ " + query + " ] was successful")
            else:
                print("Not recognised Database Query [ " + query + " ]")
        except Exception:
            print("Failed Database Query [ " + query + " ]")
            traceback.print_exc()

    def table_query(self, query):
        try:
            if re.search("CREATE", query):
                key_tmp = query.split()
                key = key_tmp[key_tmp.index("KEY") + 1]

                splitted = query.replace("(", "").replace(")", "").replace(",", "").replace("PRIMARY", "").replace("KEY", "").split()[2:]
                name = splitted[0]
                columns_mixed = splitted[1:]
                col_names = columns_mixed[::2]
                col_types = columns_mixed[1::2]

                for n, i in enumerate(col_types):
                    col_types[n] = self.get_type(col_types[n])
                self.db.create_table(name, col_names, col_types, key)
                print("CREATE Table query [ " + query + " ] was successful")
            elif re.search("DROP", query):
                self.db.drop_table(query.split(" ")[2])
                print("DROP Table query [ " + query + " ] was successful")
            elif re.search("ALTER", query):
                self.db.cast_column(query.split(" ")[2], query.split(" ")[5], self.get_type(query.split(" ")[7]))
                print("ALTER Table query [ " + query + " ] was successful")
            elif re.search("COPY", query):
                self.db.table_from_csv(query.split(" ")[1], query.split(" ")[3])
                print("COPY Table query [ " + query + " ] was successful")
            elif re.search("EXPORT", query):
                self.db.table_to_csv(query.split(" ")[1], query.split(" ")[3])
                print("EXPORT Table query [ " + query + " ] was successful")
            else:
                print("Not recognised Table Query [ " + query + " ]")
        except Exception:
            print("Failed Table Query [ " + query + " ]")
            traceback.print_exc()

    def index_query(self, query):
        try:
            if re.search("CREATE", query):
                self.db.create_index(query.replace("(", " ").split(" ")[4], query.split(" ")[2])
                print("CREATE Index query [ " + query + " ] was successful")
            elif re.search("DROP", query):
                #self.db.drop_index(query.split(" ")[2])
                #print("DROP Index query [ " + query + " ] was successful")
                print("DROP Index query [ " + query + " ] drop_index function does not exist")
            else:
                print("Not recognised Index Query [ " + query + " ]")
        except Exception:
            print("Failed Index Query [ " + query + " ]")
            traceback.print_exc()

    def insert_delete_update_query(self, query):
        try:
            if re.search("INSERT", query):
                self.db.insert(query.split(" ")[2], query.replace("(", "").replace(")", "").replace(",", "").split(" ")[4:None])
                print("INSERT query [ " + query + " ] was successful")
            elif re.search("DELETE", query):
                self.db.delete(query.split(" ")[2], query.split(" ")[4])
                print("DELETE query [ " + query + " ] was successful")
            elif re.search("UPDATE", query):
                set = query.split(" ")[3]
                self.db.update(query.split(" ")[1], set.split("=")[1], set.split("=")[0], query.split(" ")[5])
                print("UPDATE query [ " + query + " ] was successful")
            else:
                print("Not recognised Query [ " + query + " ]")
        except Exception:
            print("Failed Query [ " + query + " ]")
            traceback.print_exc()

    def select_query(self, query):
        try:
            query_array = query.replace(",", "").split(" ")

            select = query_array[query_array.index("SELECT") + 1 : query_array.index("FROM")]


            if re.search("WHERE", query):
                source_table = query_array[query_array.index("FROM") + 1 : query_array.index("WHERE")]
                condition = query_array[query_array.index("WHERE") + 1]
            else:
                source_table = query_array[query_array.index("FROM") + 1: None]
                condition = None

            top = query_array[query_array.index("TOP") + 1] if re.search("TOP", query) else None
            order_by = query_array[query_array.index("BY") + 1] if re.search("BY", query) else None
            asc = True if re.search("ASC", query) else False
            save_as = query_array[query_array.index("AS") + 1] if "AS" in query_array else None

            if 'JOIN' in source_table:
                table1 = source_table[source_table.index("INNER") - 1]
                table2 = source_table[source_table.index("JOIN") + 1]
                join_condition = source_table[source_table.index("ON") + 1]
                if condition is None:
                    self.db.inner_join(table1, table2, join_condition)
                else:
                    select = '*' if "*" in select else select
                    self.db.inner_join(table1, table2, join_condition, None, True)._select_where(select, condition, order_by, asc, top)
            else:
                select = '*' if "*" in select else select
                top = None if top is None else int(top)
                self.db.select(source_table[0], select, condition, order_by, asc, top, save_as)

            print("SELECT query [ " + query + " ] was successful")
        except Exception:
            print("Failed Select query [ " + query + " ]")
            traceback.print_exc()

def main():
    inter = SqlInterpreter()
    print("Please Type your queries. If you need to exit, please type exit()")
    while True:
        print()
        query = input()
        if query == 'exit()':
            break
        else:
            inter.interpret(query)

if __name__ == '__main__':
    main()
