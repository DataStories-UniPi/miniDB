import os, sys
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from db.database import Database
import re
import traceback


class SqlInterpreter:
    def __init__(self, db=None):
        self.db = db
        #print("Instantiated Sql Interpreter")

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
        if re.search("DATABASE", query):
            return self.database_query(query)
        elif re.search("TABLE|COPY|EXPORT", query):
            return self.table_query(query)
        elif re.search("INDEX", query):
            return self.index_query(query)
        elif re.search("INSERT|DELETE|UPDATE", query):
            return self.insert_delete_update_query(query)
        elif re.search("SELECT", query):
            return self.select_query(query)
        else:
            return "Unknown query " + query

    def database_query(self, query):
        try:
            if re.search("CREATE", query):
                self.db = Database(query.split(" ")[2], load=False)
                return "CREATE Database query [ " + query + " ] was successful"
            elif re.search("DROP", query):
                self.db = Database(query.split(" ")[2], load=True)
                self.db.drop_db()
                self.db = None
                return "DROP Database query [ " + query + " ] was successful"
            elif re.search("LOAD", query):
                self.db = Database(query.split(" ")[2], load=True)
                return "LOAD Database query [ " + query + " ] was successful"
            elif re.search("SAVE", query):
                self.db = Database(query.split(" ")[2], load=True)
                self.db.save()
                return "SAVE Database query [ " + query + " ] was successful"
            else:
                return "Not recognised Database Query [ " + query + " ]"
        except Exception:
            traceback.print_exc()
            return "Failed Database Query [ " + query + " ]"

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
                return "CREATE Table query [ " + query + " ] was successful"
            elif re.search("DROP", query):
                self.db.drop_table(query.split(" ")[2])
                return "DROP Table query [ " + query + " ] was successful"
            elif re.search("ALTER", query):
                self.db.cast_column(query.split(" ")[2], query.split(" ")[5], self.get_type(query.split(" ")[7]))
                return "ALTER Table query [ " + query + " ] was successful"
            elif re.search("COPY", query):
                self.db.table_from_csv(query.split(" ")[1], query.split(" ")[3])
                return "COPY Table query [ " + query + " ] was successful"
            elif re.search("EXPORT", query):
                self.db.table_to_csv(query.split(" ")[1], query.split(" ")[3])
                return "EXPORT Table query [ " + query + " ] was successful"
            else:
                return "Not recognised Table Query [ " + query + " ]"
        except Exception:
            traceback.print_exc()
            return "Failed Table Query [ " + query + " ]"

    def index_query(self, query):
        try:
            if re.search("CREATE", query):
                self.db.create_index(query.replace("(", " ").split(" ")[4], query.split(" ")[2])
                return "CREATE Index query [ " + query + " ] was successful"
            elif re.search("DROP", query):
                self.db.drop_index(query.split(" ")[2])
                return "DROP Index query [ " + query + " ] was successful"
                #print("DROP Index query [ " + query + " ] drop_index function does not exist")
            else:
                return "Not recognised Index Query [ " + query + " ]"
        except Exception:
            traceback.print_exc()
            return "Failed Index Query [ " + query + " ]"

    def insert_delete_update_query(self, query):
        try:
            if re.search("INSERT", query):
                self.db.insert(query.split(" ")[2], query.replace("(", "").replace(")", "").replace(",", "").split(" ")[4:None])
                return "INSERT query [ " + query + " ] was successful"
            elif re.search("DELETE", query):
                self.db.delete(query.split(" ")[2], query.split(" ")[4])
                return "DELETE query [ " + query + " ] was successful"
            elif re.search("UPDATE", query):
                set = query.split(" ")[3]
                self.db.update(query.split(" ")[1], set.split("=")[1], set.split("=")[0], query.split(" ")[5])
                return "UPDATE query [ " + query + " ] was successful"
            else:
                return "Not recognised Query [ " + query + " ]"
        except Exception:
            traceback.print_exc()
            return "Failed Query [ " + query + " ]"

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
            order_by = query_array[query_array.index("BY") + 1] if re.search("ORDER BY", query) else None
            if  re.search("GROUP BY",query) :
              if re.search("HAVING",query) :
                group_by_col_list = [col for col in query_array[query_array.index("GROUP BY") :
                query_array.index("HAVING")]] 
                having = query_array[query_array.index("HAVING") + 1] if                    re.search("HAVING",query) 
              else
                group_by_col_list = [col for col in query_array[query_array.index("GROUP  BY") :]] 
                having = None
            else 
              group_by_col_list = None
            print(group_by_col_list)            
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

            return "SELECT query [ " + query + " ] was successful"
        except Exception:
            traceback.print_exc()
            return "Failed Select query [ " + query + " ]"

def main(query=None):
    inter = SqlInterpreter()
    #print("Please Type your queries. If you need to exit, please type exit()")
    #print()
    #if query is None:
    #    query = input()
    if query != 'exit()':
        u = inter.interpret(query)
        return u

if __name__ == '__main__':
    main()
