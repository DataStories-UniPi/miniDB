from database import Database
import re
import sys

help_dialogue = '''
************HELP DIALOGUE************

miniDB is an extremely minimal DB that can be used for 
educational purposes and rapid prototyping.

Type any SQL valid command in order to interact with miniDB.

The supported commands are:
    1. SELECT-FROM-WHERE,
    2. UPDATE WHERE,
    3. INSERT INTO,
    4. DELETE FROM WHERE,
    5. CREATE|DROP|ALTER|IMPORT FROM CSV|EXPORT TO CSV TABLE, 
    6. CREATE|DROP|LOAD|SAVE DATABASE,
    7. SELECT-FROM-JOIN,
    8. CREATE|DROP INDEX ON

If you wish to print all the available databases write: "print dbs"
If you wish to exit, type: "exit".
'''

#! ISSUES

#! Add exception handling 
#! MUST DO A TEST RUN
class Interpreter:

    def __init__(self, db_name):
        '''
        The constructor for the Interpreter class.

        db_name -> The name of a database that will be created/ loaded.
        '''
        if db_name == '' or db_name == ' ' or db_name is None:
            print('\nPlease provide a database name.')
            print('The interpreter will now terminate...')
            sys.exit(0)

        self.db_name = db_name
        self.db = Database(self.db_name)


#* ΟΚ!
    def group_queries(self, query):
        '''
        Handles the initial grouping of SQL queries.
        
        query -> The SQL query that will be interpreted.
        '''
        if re.search('database|DATABASE', query):
            self.database_transactions(query)
        
        elif re.search('table|TABLE|copy|COPY', query):
            self.table_transactions(query)
        
        elif re.search('insert|update|delete|INSERT|UPDATE|DELETE', query):
            self.iud_transactions(query)
        
        elif re.search('index|INDEX', query):
            self.index_transactions(query)
        
        elif re.search('select|SELECT', query):
            self.select_transactions(query)

        else:
            print('Unknown query:', query)


#* ΟΚ!
    def get_data_types(self, raw_col_types):
        '''
        Returns the equivalent of SQL data types to Python data types.

        raw_col_types -> A list of strings with the column types of a table.
        '''
        col_types = []
        for types in raw_col_types:
            if re.search('char|varchar|tinytext|text|mediumtext|longtext|enum|set|CHAR|VARCHAR|TINYTEXT|TEXT|MEDIUMTEXT|LONGTEXT|ENUM|SET', types):
                col_types.append(str)
            elif re.search('tinyint|smallint|mediumint|int|bigint|integer|TINYINT|SMALLINT|MEDIUMINT|INT|BIGINT|INTEGER', types):
                col_types.append(int)
            elif re.search('float|double|decimal|FLOAT|DOUBLE|DECIMAL', types):
                col_types.append(float)
            elif re.search('bit|BIT', types):
                col_types.append(bytes)
            elif re.search('bool|BOOL', types):
                col_types.append(bool)
            else:
                col_types.append(str)
        
        return col_types


#* ΟΚ!
    def find_condition(self, query):
        '''
        Given a query that includes a condition this method will return the conditional expression.

        query -> The SQL query that will be interpreted.
        '''
        split_query = query.lower().split()
        for w in split_query:
            if w == 'where': 
                index = split_query.index(w)

        return split_query[index + 1]
         

#* ΟΚ!
    def database_transactions(self, query):
        '''
        Handles the creation, loading and deletion of databases.
        
        query -> The SQL query that will be interpreted.
        '''
        if re.search('create|load|CREATE|LOAD', query):
            self.db = Database(self.db_name)
        elif re.search('save|SAVE', query):
            self.db.save()
            print(f'DATABASE: { self.db_name } has been successfully saved.')
        elif re.search('drop|DROP', query):
            self.db.drop_db()
            print(f'DATABASE: { self.db_name } has been successfully dropped.')
        elif re.search('print|PRINT', query):
            self.db.print_dbs()


#* ΟΚ!
    def table_transactions(self, query): 
        '''
        Handles the creation, deletion, altering, importing from csv and exporting to csv of tables in a database.
        
        query -> The SQL query that will be interpreted.
        '''
        if re.search('create|CREATE', query):
            primary_key = None
            pk_temp_query = query.lower().replace(',', '').replace('(', ' ').replace(')', ' ').replace(';', '').split()
            if re.search('key', pk_temp_query):
                pk_index = pk_temp_query.index('key') + 1
                primary_key = pk_temp_query[pk_index]

            stripped_query = query.lower().replace(',', '').replace('(', '').replace(')', '').replace(';', '').replace('primary', '').replace('key', '').split()
            col_names = stripped_query[3:len(stripped_query) - 1:2]
            col_types = self.get_data_types(stripped_query[4::2])
            
            try:
                self.db.create_table(query.split()[2], col_names, col_types, primary_key = primary_key)
                print(f'TABLE: { query.split()[2] } has been successfully created.')
            except Exception as e:
                print(e)

        elif re.search('drop|DROP', query):
            self.db.drop_table(query.split()[2])
            print(f'TABLE: { query.split()[2] } has been dropped.')

        elif re.search('alter|ALTER', query):
            data_type = self.get_data_types(query.split()[7])
            self.db.cast_column(query.split()[2], query.split()[5], data_type)

        elif re.search('copy|COPY', query):
            # all column types are of string type by default and no primary key is added
            filename = input('Please enter the filename:')
            self.db.table_from_csv(filename, name = query.split()[1])            
            

        elif re.search('export|EXPORT', query):
            filename = input('Please enter the filename:')
            if '.csv' in filename:
                self.db.table_to_csv(query.split()[2], filename)
            else:
                self.db.table_to_csv(query.split()[2], filename + '.csv')


#* ΟΚ!
    def iud_transactions(self, query):
        '''
        Handles inserting, updating and deleting values in tables.
        iud = Insert Update Delete
        
        query -> The SQL query that will be interpreted.
        '''
        if re.search('insert|INSERT', query):
            stripped_query = query.lower().replace(',', '').replace('(', '').replace(')', '').replace(';', '')
            index = stripped_query.index('values') + 6
            self.db.insert(query.split()[2], stripped_query[index:].split())

        elif re.search('update|UPDATE', query):
            condition = self.find_condition(query)
            column, value = query.split()[3].replace('=', ' ').split()
            self.db.update(query.split()[1], value, column, condition)

        elif re.search('delete|DELETE', query):
            condition = self.find_condition(query)            
            self.db.delete(query.split()[2], condition)


#* ΟΚ!
    def index_transactions(self, query): 
        '''
        Handles the creation and deletion of indices on tables.
        Default index type: B+ Tree.

        query -> The SQL query that will be interpreted.
        '''
        if re.search('create|CREATE', query):
            # the index type is not specified because the supported index is B+ Tree.
            # if at some point more indices are supported just specify the type.
            self.db.create_index(query.replace('(', ' ').split()[4], query.split()[2])
            print(f'INDEX: { query.split()[2] } has been successfully created on: { query.replace("(", " ").split()[4] }.')
        elif re.search('drop|DROP', query):
            self.db.drop_index(query.split()[2])
            print(f'INDEX: { query.split()[2] } has been successfully dropped.')


    def select_transactions(self, query):
        '''
        Handles the selection of values from a table.

        query -> The SQL query that will be interpreted.
        '''
        if re.search('join|JOIN', query):
            if re.search('where|WHERE', query):
                condition = self.find_condition(query)
                self.db.inner_join(query.split()[3], query.split()[6], condition, return_object=True)._select_where()
            else:
                condition = self.find_condition(query)
                self.db.inner_join(query.split()[3], query.split()[6], condition)
        else:
            self.db.select()


                            ######## Driver Code ########
def main(): 
    '''
    This is the main function that handles the flow of the interpreter.
    '''
    global help_dialogue
    print('SQL Interpreter for miniDB.')
    print('Type a query, "help" for help or "exit" to exit.')
    db_name = input('\nPlease provide the name of the database you want to connect to or create: ')
    interpreter = Interpreter(db_name)

    while True:
        user_cmd = input('>>> ')
        if user_cmd == 'help':
            print(help_dialogue)
        elif user_cmd == 'exit':
            print('Terminating the Interpreter...')
            sys.exit(0)
        else:
            interpreter.group_query(user_cmd)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nTerminating the Interpreter...')
        sys.exit(0)