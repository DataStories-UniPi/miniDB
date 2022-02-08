from __future__ import annotations
import pickle
from table import Table
from time import sleep, localtime, strftime
import os,sys
from btree import Btree
import shutil
from misc import split_condition
import logging
import warnings
import readline
from tabulate import tabulate


# sys.setrecursionlimit(100)

# Clear command cache (journal)
readline.clear_history()

class Database:
    '''
    Main Database class, containing tables.
    '''

    def __init__(self, name, load=True):
        self.tables = {}
        self._name = name

        self.savedir = f'dbdata/{name}_db'

        if load:
            try:
                self.load_database()
                logging.info(f'Loaded "{name}".')
                return
            except:
                warnings.warn(f'Database "{name}" does not exist. Creating new.')

        # create dbdata directory if it doesnt exist
        if not os.path.exists('dbdata'):
            os.mkdir('dbdata')

        # create new dbs save directory
        try:
            os.mkdir(self.savedir)
        except:
            pass

        # create all the meta tables
        self.create_table('meta_length', 'table_name,no_of_rows', 'str,int')
        self.create_table('meta_locks', 'table_name,pid,mode', 'str,int,str')
        self.create_table('meta_insert_stack', 'table_name,indexes', 'str,list')
        self.create_table('meta_indexes', 'table_name,index_name', 'str,str')
        #
        # Creating a new meta table that will contain all the relations about parent
        # and child tables, when a column on a table references another column
        # on another table.
        #
        self.create_table('meta_parent_child_tables', 'parent_table,parent_column,child_table,child_column', 'str,str,str,str')

        self.save_database()

    def save_database(self):
        '''
        Save database as a pkl file. This method saves the database object, including all tables and attributes.
        '''
        for name, table in self.tables.items():
            with open(f'{self.savedir}/{name}.pkl', 'wb') as f:
                pickle.dump(table, f)

    def _save_locks(self):
        '''
        Stores the meta_locks table to file as meta_locks.pkl.
        '''
        with open(f'{self.savedir}/meta_locks.pkl', 'wb') as f:
            pickle.dump(self.tables['meta_locks'], f)

    def load_database(self):
        '''
        Load all tables that are part of the database (indices noted here are loaded).

        Args:
            path: string. Directory (path) of the database on the system.
        '''
        path = f'dbdata/{self._name}_db'
        for file in os.listdir(path):

            if file[-3:]!='pkl': # if used to load only pkl files
                continue
            f = open(path+'/'+file, 'rb')
            tmp_dict = pickle.load(f)
            f.close()
            name = f'{file.split(".")[0]}'
            self.tables.update({name: tmp_dict})
            # setattr(self, name, self.tables[name])

    #### IO ####

    def _update(self):
        '''
        Update all meta tables.
        '''
        self._update_meta_length()
        self._update_meta_insert_stack()

    #
    # added references=None at the end of the function's parameters
    # removed load=None,
    #
    def create_table(self, name, column_names, column_types, primary_key=None, references=None):
        '''
        This method create a new table. This table is saved and can be accessed via db_object.tables['table_name'] or db_object.table_name

        Args:
            name: string. Name of table.
            column_names: list. Names of columns.
            column_types: list. Types of columns.
            primary_key: string. The primary key (if it exists).
            load: boolean. Defines table object parameters as the name of the table and the column names.
            references: Defines the foreign key of this table, and shows the primary key of another table
        '''
        # print('here -> ', column_names.split(','))
        #
        # added references=references at the end
        # removed load=load,
        #
        #print("Database.references = ", references)
        self.tables.update({name: Table(name=name, column_names=column_names.split(','), column_types=column_types.split(','), primary_key=primary_key, references=references)})
        # self._name = Table(name=name, column_names=column_names, column_types=column_types, load=load)
        # check that new dynamic var doesnt exist already
        # self.no_of_tables += 1

        ref = str(references)
        ref = ref.split(',')
        if references is not None:
            self._update_meta_parent_child_tables(ref[0], ref[1], name, ref[2])
        self._update()
        self.save_database()
        # (self.tables[name])
        print(f'Created table "{name}".')


    def drop_table(self, table_name):
        '''
        Drop table from current database.

        Args:
            table_name: string. Name of table.
        '''
        self.load_database()
        self.lock_table(table_name)

        self.tables.pop(table_name)
        if os.path.isfile(f'{self.savedir}/{table_name}.pkl'):
            os.remove(f'{self.savedir}/{table_name}.pkl')
        else:
            warnings.warn(f'"{self.savedir}/{table_name}.pkl" not found.')
        self.delete_from('meta_locks', f'table_name={table_name}')
        self.delete_from('meta_length', f'table_name={table_name}')
        self.delete_from('meta_insert_stack', f'table_name={table_name}')

        # self._update()
        self.save_database()

    #
    # added references=None at the end of the function's parameters
    #
    def import_table(self, table_name, filename, column_types=None, primary_key=None, references=None):
        '''
        Creates table from CSV file.

        Args:
            filename: string. CSV filename. If not specified, filename's name will be used.
            column_types: list. Types of columns. If not specified, all will be set to type str.
            primary_key: string. The primary key (if it exists).
        '''
        file = open(filename, 'r')

        first_line=True
        for line in file.readlines():
            if first_line:
                colnames = line.strip('\n')
                if column_types is None:
                    column_types = ",".join(['str' for _ in colnames.split(',')])
                #
                # added references=references at the end
                #
                self.create_table(name=table_name, column_names=colnames, column_types=column_types, primary_key=primary_key, references=references)
                lock_ownership = self.lock_table(table_name, mode='x')
                first_line = False
                continue
            self.tables[table_name]._insert(line.strip('\n').split(','))

        if lock_ownership:
             self.unlock_table(table_name)
        self._update()
        self.save_database()


    def export(self, table_name, filename=None):
        '''
        Transform table to CSV.

        Args:
            table_name: string. Name of table.
            filename: string. Output CSV filename.
        '''
        res = ''
        for row in [self.tables[table_name].column_names]+self.tables[table_name].data:
            res+=str(row)[1:-1].replace('\'', '').replace('"','').replace(' ','')+'\n'

        if filename is None:
            filename = f'{table_name}.csv'

        with open(filename, 'w') as file:
           file.write(res)

    def table_from_object(self, new_table):
        '''
        Add table object to database.

        Args:
            new_table: string. Name of new table.
        '''

        self.tables.update({new_table._name: new_table})
        if new_table._name not in self.__dir__():
            setattr(self, new_table._name, new_table)
        else:
            raise Exception(f'"{new_table._name}" attribute already exists in class "{self.__class__.__name__}".')
        self._update()
        self.save_database()



    ##### table functions #####

    # In every table function a load command is executed to fetch the most recent table.
    # In every table function, we first check whether the table is locked. Since we have implemented
    # only the X lock, if the tables is locked we always abort.
    # After every table function, we update and save. Update updates all the meta tables and save saves all
    # tables.

    # these function calls are named close to the ones in postgres

    def cast(self, column_name, table_name, cast_type):
        '''
        Modify the type of the specified column and cast all prexisting values.
        (Executes type() for every value in column and saves)

        Args:
            table_name: string. Name of table (must be part of database).
            column_name: string. The column that will be casted (must be part of database).
            cast_type: type. Cast type (do not encapsulate in quotes).
        '''
        self.load_database()

        lock_ownership = self.lock_table(table_name, mode='x')
        self.tables[table_name]._cast_column(column_name, eval(cast_type))
        if lock_ownership:
            self.unlock_table(table_name)
        self._update()
        self.save_database()

    def insert_into(self, table_name, row_str):
        '''
        Inserts data to given table.

        Args:
            table_name: string. Name of table (must be part of database).
            row: list. A list of values to be inserted (will be casted to a predifined type automatically).
            lock_load_save: boolean. If False, user needs to load, lock and save the states of the database (CAUTION). Useful for bulk-loading.
        '''
        # This contains all the row values that are being inserted
        row = row_str.strip().split(',')

        #x = self.tables.column_names
        #
        # Here i will call the function _check_meta_parent_child_tables_for_insert
        # to see if we insert something on a parent or child table
        #
        self._check_meta_parent_child_tables_for_insert(row,table_name,"insert")

        self.load_database()
        # fetch the insert_stack. For more info on the insert_stack
        # check the insert_stack meta table
        lock_ownership = self.lock_table(table_name, mode='x')
        insert_stack = self._get_insert_stack_for_table(table_name)
        try:
            self.tables[table_name]._insert(row, insert_stack)
        except Exception as e:
            logging.info(e)
            logging.info('ABORTED')
        self._update_meta_insert_stack_for_tb(table_name, insert_stack[:-1])

        if lock_ownership:
            self.unlock_table(table_name)
        self._update()
        self.save_database()
        # Confirmation message
        print("Row is inserted into the table successfully!")


    def update_table(self, table_name, set_args, condition):
        '''
        Update the value of a column where a condition is met.

        Args:
            table_name: string. Name of table (must be part of database).
            set_value: string. New value of the predifined column name.
            set_column: string. The column to be altered.
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.

                Operatores supported: (<,<=,==,>=,>)
        '''


        for_function_set_args = set_args.split('=')
        for_function_condition = condition.split('=')
        #
        # Callin this function to check for referential constraints
        #
        updating_a_parent_table = self._check_meta_parent_child_tables_for_update_in_child_table(table_name,for_function_set_args,for_function_condition)

        set_column, set_value = set_args.replace(' ','').split('=')
        self.load_database()

        lock_ownership = self.lock_table(table_name, mode='x')
        self.tables[table_name]._update_rows(set_value, set_column, condition)
        if lock_ownership:
            self.unlock_table(table_name)
        self._update()
        self.save_database()
        if updating_a_parent_table == True:
            print("!!!!ATTENTION!!!!")
            print("You just updated a value of a column that other column values are linked to it.")
            print("The system will now automatically change every value that references the old value,")
            print("and it will be converted to the current new value")
            self._change_referencing_values_everywhere(table_name,for_function_set_args,for_function_condition)
        # Confirmation message
        print("Table updated successfully!")

    def delete_from(self, table_name, condition):
        '''
        Delete rows of table where condition is met.

        Args:
            table_name: string. Name of table (must be part of database).
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.

                Operatores supported: (<,<=,==,>=,>)
        '''
        self.load_database()

        lock_ownership = self.lock_table(table_name, mode='x')
        deleted = self.tables[table_name]._delete_where(condition)
        if lock_ownership:
            self.unlock_table(table_name)
        self._update()
        self.save_database()
        # we need the save above to avoid loading the old database that still contains the deleted elements
        if table_name[:4]!='meta':
            self._add_to_insert_stack(table_name, deleted)
        self.save_database()

    def select(self, columns, table_name, condition, order_by=None, top_k=True,\
               desc=None, save_as=None, return_object=True):
        '''
        Selects and outputs a table's data where condtion is met.

        Args:
            table_name: string. Name of table (must be part of database).
            columns: list. The columns that will be part of the output table (use '*' to select all available columns)
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.

                Operatores supported: (<,<=,==,>=,>)
            order_by: string. A column name that signals that the resulting table should be ordered based on it (no order if None).
            desc: boolean. If True, order_by will return results in descending order (True by default).
            top_k: int. An integer that defines the number of rows that will be returned (all rows if None).
            save_as: string. The name that will be used to save the resulting table into the database (no save if None).
            return_object: boolean. If True, the result will be a table object (useful for internal use - the result will be printed by default).
        '''
        # print(table_name)
        self.load_database()
        if isinstance(table_name,Table):
            return table_name._select_where(columns, condition, order_by, desc, top_k)

        if condition is not None:
            condition_column = split_condition(condition)[0]
        else:
            condition_column = ''


        # self.lock_table(table_name, mode='x')
        if self.is_locked(table_name):
            return
        if self._has_index(table_name) and condition_column==self.tables[table_name].column_names[self.tables[table_name].pk_idx]:
            index_name = self.select('*', 'meta_indexes', f'table_name={table_name}', return_object=True).column_by_name('index_name')[0]
            bt = self._load_idx(index_name)
            table = self.tables[table_name]._select_where_with_btree(columns, bt, condition, order_by, desc, top_k)
        else:
            table = self.tables[table_name]._select_where(columns, condition, order_by, desc, top_k)
        # self.unlock_table(table_name)
        if save_as is not None:
            table._name = save_as
            self.table_from_object(table)
        else:
            if return_object:
                return table
            else:
                return table.show()


    def show_table(self, table_name, no_of_rows=None):
        '''
        Print table in a readable tabular design (using tabulate).

        Args:
            table_name: string. Name of table (must be part of database).
        '''
        self.load_database()

        self.tables[table_name].show(no_of_rows, self.is_locked(table_name))


    def sort(self, table_name, column_name, asc=False):
        '''
        Sorts a table based on a column.

        Args:
            table_name: string. Name of table (must be part of database).
            column_name: string. the column name that will be used to sort.
            asc: If True sort will return results in ascending order (False by default).
        '''

        self.load_database()

        lock_ownership = self.lock_table(table_name, mode='x')
        self.tables[table_name]._sort(column_name, asc=asc)
        if lock_ownership:
            self.unlock_table(table_name)
        self._update()
        self.save_database()

    def join(self, mode, left_table, right_table, condition, save_as=None, return_object=True):
        '''
        Join two tables that are part of the database where condition is met.

        Args:
            left_table: string. Name of the left table (must be in DB) or Table obj.
            right_table: string. Name of the right table (must be in DB) or Table obj.
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.

                Operatores supported: (<,<=,==,>=,>)
        save_as: string. The output filename that will be used to save the resulting table in the database (won't save if None).
        return_object: boolean. If True, the result will be a table object (useful for internal usage - the result will be printed by default).
        '''
        self.load_database()
        if self.is_locked(left_table) or self.is_locked(right_table):
            return

        left_table = left_table if isinstance(left_table, Table) else self.tables[left_table]
        right_table = right_table if isinstance(right_table, Table) else self.tables[right_table]


        if mode=='inner':
            res = left_table._inner_join(right_table, condition)
        else:
            raise NotImplementedError

        if save_as is not None:
            res._name = save_as
            self.table_from_object(res)
        else:
            if return_object:
                return res
            else:
                res.show()

    def lock_table(self, table_name, mode='x'):
        '''
        Locks the specified table using the exclusive lock (X).

        Args:
            table_name: string. Table name (must be part of database).
        '''
        if table_name[:4]=='meta' or table_name not in self.tables.keys() or isinstance(table_name,Table):
            return

        with open(f'{self.savedir}/meta_locks.pkl', 'rb') as f:
            self.tables.update({'meta_locks': pickle.load(f)})

        try:
            pid = self.tables['meta_locks']._select_where('pid',f'table_name={table_name}').data[0][0]
            if pid!=os.getpid():
                raise Exception(f'Table "{table_name}" is locked by process with pid={pid}')
            else:
                return False

        except IndexError:
            pass

        if mode=='x':
            self.tables['meta_locks']._insert([table_name, os.getpid(), mode])
        else:
            raise NotImplementedError
        self._save_locks()
        return True
        # print(f'Locking table "{table_name}"')

    def unlock_table(self, table_name, force=False):
        '''
        Unlocks the specified table that is exclusively locked (X).

        Args:
            table_name: string. Table name (must be part of database).
        '''
        if table_name not in self.tables.keys():
            raise Exception(f'Table "{table_name}" is not in database')

        if not force:
            try:
                # pid = self.select('*','meta_locks',  f'table_name={table_name}', return_object=True).data[0][1]
                pid = self.tables['meta_locks']._select_where('pid',f'table_name={table_name}').data[0][0]
                if pid!=os.getpid():
                    raise Exception(f'Table "{table_name}" is locked by the process with pid={pid}')
            except IndexError:
                pass
        self.tables['meta_locks']._delete_where(f'table_name={table_name}')
        self._save_locks()
        # print(f'Unlocking table "{table_name}"')

    def is_locked(self, table_name):
        '''
        Check whether the specified table is exclusively locked (X).

        Args:
            table_name: string. Table name (must be part of database).
        '''
        if isinstance(table_name,Table) or table_name[:4]=='meta':  # meta tables will never be locked (they are internal)
            return False

        with open(f'{self.savedir}/meta_locks.pkl', 'rb') as f:
            self.tables.update({'meta_locks': pickle.load(f)})

        try:
            pid = self.tables['meta_locks']._select_where('pid',f'table_name={table_name}').data[0][0]
            if pid!=os.getpid():
                raise Exception(f'Table "{table_name}" is locked by the process with pid={pid}')

        except IndexError:
            pass
        return False

    def journal(idx = None):
        if idx != None:
            cache_list = '\n'.join([str(readline.get_history_item(i + 1)) for i in range(readline.get_current_history_length())]).split('\n')[int(idx)]
            out = tabulate({"Command": cache_list.split('\n')}, headers=["Command"])
        else:
            cache_list = '\n'.join([str(readline.get_history_item(i + 1)) for i in range(readline.get_current_history_length())])
            out = tabulate({"Command": cache_list.split('\n')}, headers=["Index","Command"], showindex="always")
        print('journal:', out)
        #return out


    #### META ####

    # The following functions are used to update, alter, load and save the meta tables.
    # Important: Meta tables contain info regarding the NON meta tables ONLY.
    # i.e. meta_length will not show the number of rows in meta_locks etc.

    def _update_meta_length(self):
        '''
        Updates the meta_length table.
        '''
        for table in self.tables.values():
            if table._name[:4]=='meta': #skip meta tables
                continue
            if table._name not in self.tables['meta_length'].column_by_name('table_name'): # if new table, add record with 0 no. of rows
                self.tables['meta_length']._insert([table._name, 0])

            # the result needs to represent the rows that contain data. Since we use an insert_stack
            # some rows are filled with Nones. We skip these rows.
            non_none_rows = len([row for row in table.data if any(row)])
            self.tables['meta_length']._update_rows(non_none_rows, 'no_of_rows', f'table_name={table._name}')
            # self.update_row('meta_length', len(table.data), 'no_of_rows', 'table_name', '==', table._name)

    def _update_meta_locks(self):
        '''
        Updates the meta_locks table.
        '''
        for table in self.tables.values():
            if table._name[:4]=='meta': #skip meta tables
                continue
            if table._name not in self.tables['meta_locks'].column_by_name('table_name'):

                self.tables['meta_locks']._insert([table._name, False])
                # self.insert('meta_locks', [table._name, False])

    def _update_meta_insert_stack(self):
        '''
        Updates the meta_insert_stack table.
        '''
        for table in self.tables.values():
            if table._name[:4]=='meta': #skip meta tables
                continue
            if table._name not in self.tables['meta_insert_stack'].column_by_name('table_name'):
                self.tables['meta_insert_stack']._insert([table._name, []])
    def _update_meta_parent_child_tables(self,parent_table,parent_column,child_table,child_column):
        '''
        This function can only be called by the "create_table()" function when a new table is
        being created and its job is to insert a new row to the meta_parent_child_tables table
        that contains:
            1) The name of the parent table (the one that is being referenced by the child table)
            2) The name of the parent column (the one that is being referenced by the child column)
            3) The name of the child table (the one that is referencing the parent table)
            4) The name of the child column (the one that is referencing the parent column)
        '''

        self.tables['meta_parent_child_tables']._insert([parent_table, parent_column, child_table, child_column])

    def _check_meta_parent_child_tables_for_insert(self,row_values,table_name,function_executed):
        '''
        This specific function will be called every time in 4  occasions:
            1) When i insert a new row in a child table
            2) When i update a value in a child table
            3) When i update a value in a parent table
            4) When i delete a value in a parent table

        The job of this function is to check in each of these 4 occasions if the
        current query is inserting/updating/deleting a value in a child/parent table

        If that is true, then i will take care of the referential constraints
        If that is not true, an error message will be popped!
        '''
        # This contains all the data from the current table in a 2D list
        data_in_table =  self.tables[table_name].data

        #
        # a list that contains the data of the meta_parent_child_tables table
        # in 2D list
        #
        parent_child_data = self.tables['meta_parent_child_tables'].data

        # a list that will contain all the child column names of the database
        child_columns_list = []

        # a list that will contain all the parent column names of the database
        parent_columns_list = []

        # a list that contains the column names of the current table
        current_table_columns = self.tables[table_name].column_names

        # a list that will contain all the child table names of the database
        child_tables_list = []

        # a list that will contain all the parent table names of the database
        parent_tables_list = []

        #
        # Putting into a list all the names of the parent tables and columns, and
        # also putting into a list all the names of the child tables and columns
        #
        for i in range(len(parent_child_data)):
            for j in range(len(parent_child_data[i])):
                if j == 0:
                    parent_tables_list.append(parent_child_data[i][j])
                if j == 1:
                    parent_columns_list.append(parent_child_data[i][j])
                if j == 2:
                    child_tables_list.append(parent_child_data[i][j])
                if j == 3:
                    child_columns_list.append(parent_child_data[i][j])


        for table in self.tables.values():

            if table._name[:4]=='meta': #skip meta tables
                continue
            if table_name in self.tables['meta_parent_child_tables'].column_by_name('parent_table'):
                #print(table_name,"is in parent_table column")
                #
                # In this part of the function i will take care of the executed query
                # when it belongs in the column "parent_table" in the table "meta_parent_child_tables"
                #

                if function_executed == "update":
                    print("doing some stuff about update")

            elif table_name in self.tables['meta_parent_child_tables'].column_by_name('child_table'):
                #
                # In this part of the function i will take care of the executed query
                # when it belongs in the column "child_table" in the table "meta_parent_child_tables"
                #

                if function_executed == "insert":
                    #
                    # When we insert a new row in a child column, then we should
                    # check if this column has always a value that already exists
                    # in the parent column
                    #

                    for i in range(len(current_table_columns)):
                        # If i find a column from the current table, in the list
                        # of child columns
                        if current_table_columns[i] in child_columns_list:
                            # I will try to see if this value also exist in the
                            # referenced column of the referenced table
                            value_to_be_checked = row_values[i]
                            referenced_column = parent_columns_list[child_columns_list.index(current_table_columns[i])]
                            referenced_table = parent_tables_list[child_columns_list.index(current_table_columns[i])]
                            values_in_referenced_column = self.tables[referenced_table].column_by_name(referenced_column)

                            # Trying to find if the new inserted value exists in the referenced table
                            found_it = False
                            for v in values_in_referenced_column:
                                if str(value_to_be_checked) == str(v):
                                    found_it = True

                            # Error message for when the referencial constraints are not met
                            if found_it == False:
                                raise Exception("Eror!!!\n***************************************************** \nOne of the values you try to insert, do not match the referenced column value\n*****************************************************")




            else:
                print("This table does not have any referential constraints")
                print("Inserting normally...")
            break


    def _check_meta_parent_child_tables_for_update_in_child_table(self,table_name,set_args,condition):
        '''
        This specific function will be called every time the user tries to update a value.
        When an update is about to happen, then:
            1) If the update is on a table with no referential constraints, then
               nothing will happen here
            2) If the update is on a value X from a column that references another value Y
               in another column(we call this type of column a child_column), then we check here
               if the new value exists in the referenced column.
               If not, then en error message will be popped
            3) If the update is on a value X from a column that is  referenced from another value Y
               in another column(we call this type of column a parent_column), then we make
               sure here, that boolean variable is set to 'True' and this is the only time
               that tis function will return 'True' and not the default 'False'

        If the function returns 'True', then another function that we made will be called
        through the "update table()" function, the function "_change_referencing_values_everywhere()"
        that deals with the 3rd situation

        The executed query in an "update" query must look something like that:
            "update table ship_parking set parked_ship_id=102 where parked_ship_id=10222;"
        and the args in this function will be:
            table name: ship_parking
            set_args: ['parked_ship_id', '102']
            condition: ['parked_ship_id', '10222']

        '''
        # This is what this function will return
        updating_a_parent_table = False

        #
        # a list that contains the data of the meta_parent_child_tables table
        # in 2D list
        #
        parent_child_data = self.tables['meta_parent_child_tables'].data

        # a list that will contain all the child column names of the database
        child_columns_list = []

        # a list that will contain all the parent column names of the database
        parent_columns_list = []

        # a list that contains the column names of the current table
        current_table_columns = self.tables[table_name].column_names

        # a list that will contain all the child table names of the database
        child_tables_list = []

        # a list that will contain all the parent table names of the database
        parent_tables_list = []

        # Getting the values of the child column in this current table
        values_of_child_column = self.tables[table_name].column_by_name(set_args[0])

        # Error message for when we try to search a value that does not exist
        if str(condition[1]) not in str(values_of_child_column):
            raise Exception("Error!!!\n***************************************************** \nThere is not such a value in the table you try to update. \nThe \"where\" condition is wrong \nCheck it and try again\n*****************************************************")


        #
        # Putting into a list all the names of the parent tables and columns, and
        # also putting into a list all the names of the child tables and columns
        #
        for i in range(len(parent_child_data)):
            for j in range(len(parent_child_data[i])):
                if j == 0:
                    parent_tables_list.append(parent_child_data[i][j])
                if j == 1:
                    parent_columns_list.append(parent_child_data[i][j])
                if j == 2:
                    child_tables_list.append(parent_child_data[i][j])
                if j == 3:
                    child_columns_list.append(parent_child_data[i][j])

        #
        # First i am going to check everything for the case when we
        # update a column in a child table
        #
        if str(set_args[0]) in child_columns_list:
            #print(set_args[0],"is in child columns list")
            # Getting the name of the parent table
            parent_table_to_be_checked = parent_tables_list[child_columns_list.index(set_args[0])]
            # Getting the name of the child table
            parent_column_to_be_checked = parent_columns_list[child_columns_list.index(set_args[0])]
            # Getting the values of the parent column
            values_of_parent_column = self.tables[parent_table_to_be_checked].column_by_name(parent_column_to_be_checked)

            # Trying to find if the new updated value exists in the referenced table
            value_to_be_checked = set_args[1]
            found_it = False
            for v in values_of_parent_column:
                if str(value_to_be_checked) == str(v):
                    found_it = True

            # Error message for when the referencial constraints are not met
            if found_it == False:
                raise Exception("Error!!!\n*****************************************************\nThis update can not occur due to referential constraints \nThe value you try to update, do not match any of the referenced column values\n*****************************************************")

        #
        # Secondly, i am going to check everything for the case when we
        # update a column in a child table
        #
        elif str(set_args[0]) in parent_columns_list:
            # The function now will return true, and this means
            # that another function will be called to take care of the referential
            # constraints for the specific situation.
            # By situation i mean when i update a parent_column vallue
            updating_a_parent_table = True
            # Getting the values of the parent column in this current table
            values_of_parent_column = self.tables[table_name].column_by_name(set_args[0])

            # Error message for when we try to search a value that does not exist
            if str(condition[1]) not in str(values_of_parent_column):
                raise Exception("Error!!!\n***************************************************** \nThere is not such a value in the table you try to update. \nThe \"where\" condition is wrong \nCheck it and try again\n*****************************************************")

        # True if we update a parent table
        # False if we dont
        return updating_a_parent_table

    def _change_referencing_values_everywhere(self,table_name,set_args,condition):
        '''
        This function will only be called if the user tries to update a value X
        that other values Y refer to it
        Its job is to update the value X and to change all other values Y that are childs of X
        In the end, X and all the Ys will all have the same value
        '''
        #
        # a list that contains the data of the meta_parent_child_tables table
        # in 2D list
        #
        parent_child_data = self.tables['meta_parent_child_tables'].data

        # a list that will contain all the child column names of the database
        child_columns_list = []

        # a list that will contain all the parent column names of the database
        parent_columns_list = []

        # a list that contains the column names of the current table
        current_table_columns = self.tables[table_name].column_names

        # a list that will contain all the child table names of the database
        child_tables_list = []

        # a list that will contain all the parent table names of the database
        parent_tables_list = []


        #
        # Putting into a list all the names of the parent tables and columns, and
        # also putting into a list all the names of the child tables and columns
        #
        for i in range(len(parent_child_data)):
            for j in range(len(parent_child_data[i])):
                if j == 0:
                    parent_tables_list.append(parent_child_data[i][j])
                if j == 1:
                    parent_columns_list.append(parent_child_data[i][j])
                if j == 2:
                    child_tables_list.append(parent_child_data[i][j])
                if j == 3:
                    child_columns_list.append(parent_child_data[i][j])


        referenced_column = set_args[0]
        referencing_column = child_columns_list[parent_columns_list.index(referenced_column)]
        referencing_table = child_tables_list[parent_columns_list.index(referenced_column)]
        new_referencing_value = set_args[1]
        old_referencing_value = condition[1]
        values_of_child_column = self.tables[child_tables_list[parent_columns_list.index(referenced_column)]].column_by_name(child_columns_list[parent_columns_list.index(referenced_column)])

        if str(old_referencing_value) in values_of_child_column:
            new_set_arg = referencing_column + "=" + new_referencing_value
            new_condition = referencing_column + "=" + old_referencing_value
            # Calling the function to change everything related to the old value
            self.update_table(referencing_table,new_set_arg,new_condition)







    def _add_to_insert_stack(self, table_name, indexes):
        '''
        Adds provided indices to the insert stack of the specified table.

        Args:
            table_name: string. Table name (must be part of database).
            indexes: list. The list of indices that will be added to the insert stack (the indices of the newly deleted elements).
        '''
        old_lst = self._get_insert_stack_for_table(table_name)
        self._update_meta_insert_stack_for_tb(table_name, old_lst+indexes)

    def _get_insert_stack_for_table(self, table_name):
        '''
        Returns the insert stack of the specified table.

        Args:
            table_name: string. Table name (must be part of database).
        '''
        return self.tables['meta_insert_stack']._select_where('*', f'table_name={table_name}').column_by_name('indexes')[0]
        # res = self.select('meta_insert_stack', '*', f'table_name={table_name}', return_object=True).indexes[0]
        # return res

    def _update_meta_insert_stack_for_tb(self, table_name, new_stack):
        '''
        Replaces the insert stack of a table with the one supplied by the user.

        Args:
            table_name: string. Table name (must be part of database).
            new_stack: string. The stack that will be used to replace the existing one.
        '''
        self.tables['meta_insert_stack']._update_rows(new_stack, 'indexes', f'table_name={table_name}')


    # indexes
    def create_index(self, index_name, table_name, index_type='btree'):
        '''
        Creates an index on a specified table with a given name.
        Important: An index can only be created on a primary key (the user does not specify the column).

        Args:
            table_name: string. Table name (must be part of database).
            index_name: string. Name of the created index.
        '''
        if self.tables[table_name].pk_idx is None: # if no primary key, no index
            raise Exception('Cannot create index. Table has no primary key.')
        if index_name not in self.tables['meta_indexes'].column_by_name('index_name'):
            # currently only btree is supported. This can be changed by adding another if.
            if index_type=='btree':
                logging.info('Creating Btree index.')
                # insert a record with the name of the index and the table on which it's created to the meta_indexes table
                self.tables['meta_indexes']._insert([table_name, index_name])
                # crate the actual index
                self._construct_index(table_name, index_name)
                self.save_database()
        else:
            raise Exception('Cannot create index. Another index with the same name already exists.')

    def _construct_index(self, table_name, index_name):
        '''
        Construct a btree on a table and save.

        Args:
            table_name: string. Table name (must be part of database).
            index_name: string. Name of the created index.
        '''
        bt = Btree(3) # 3 is arbitrary

        # for each record in the primary key of the table, insert its value and index to the btree
        for idx, key in enumerate(self.tables[table_name].column_by_name(self.tables[table_name].pk)):
            bt.insert(key, idx)
        # save the btree
        self._save_index(index_name, bt)


    def _has_index(self, table_name):
        '''
        Check whether the specified table's primary key column is indexed.

        Args:
            table_name: string. Table name (must be part of database).
        '''
        return table_name in self.tables['meta_indexes'].column_by_name('table_name')

    def _save_index(self, index_name, index):
        '''
        Save the index object.

        Args:
            index_name: string. Name of the created index.
            index: obj. The actual index object (btree object).
        '''
        try:
            os.mkdir(f'{self.savedir}/indexes')
        except:
            pass

        with open(f'{self.savedir}/indexes/meta_{index_name}_index.pkl', 'wb') as f:
            pickle.dump(index, f)

    def _load_idx(self, index_name):
        '''
        Load and return the specified index.

        Args:
            index_name: string. Name of created index.
        '''
        f = open(f'{self.savedir}/indexes/meta_{index_name}_index.pkl', 'rb')
        index = pickle.load(f)
        f.close()
        return index
