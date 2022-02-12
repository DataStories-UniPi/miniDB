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
        # flag for when transaction initiation starts
        self.save_state = False
        self.savedir = f'dbdata/{name}_db'
        # to keep locked tables allowing us to unlock them at the end of the transaction
        self.transaction_locks=[]
        if load:
            try:
                self.load_database()
                logging.info(f'Loaded "{name}".')
                for i in self.tables.keys():
                    if not self.is_locked(i):
                        self.unlock_table(i, True)
                return
            except pickle.PickleError:
                warnings.warn(f'Database "{name}" does not exist. Creating new.')
            except:
                warnings.warn('a table is locked')
                return

        # create dbdata directory if it doesnt exist
        if not os.path.exists('dbdata'):
            os.mkdir('dbdata')

        # create new dbs save directory
        try:
            os.mkdir(self.savedir)
        except:
            pass

        # create all the meta tables
        # table for saving triggers and selecting them .
        # meta_triggers fields 
        # table_name -> table affected by trigger
        # trigger_name -> pretty self explanatory
        # when -> timing prin meta ktlp...
        # act -> what action shall be taken
        # func_name -> file name/name of function
        self.create_table('meta_triggers', 'table_name,trigger_name,when,act,func_name', 'str,str,str,str,str','trigger_name')
        self.create_table('meta_length', 'table_name,no_of_rows', 'str,int')
        self.create_table('meta_locks', 'table_name,pid,mode', 'str,int,str')
        self.create_table('meta_insert_stack', 'table_name,indexes', 'str,list')
        self.create_table('meta_indexes', 'table_name,index_name', 'str,str')
        self.save_database()

    # altered :
    # for save_database and load_database we added an if as to not save so the database does not have 
    # to be saved when we start_transaction as per ticket #82
    def save_database(self):
        '''
        Save database as a pkl file. This method saves the database object, including all tables and attributes.
        '''
        if not self.save_state:
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
        if not self.save_state:
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


    def create_table(self, name, column_names, column_types, primary_key=None, load=None):
        '''
        This method create a new table. This table is saved and can be accessed via db_object.tables['table_name'] or db_object.table_name

        Args:
            name: string. Name of table.
            column_names: list. Names of columns.
            column_types: list. Types of columns.
            primary_key: string. The primary key (if it exists).
            load: boolean. Defines table object parameters as the name of the table and the column names.
        '''
        # print('here -> ', column_names.split(','))
        self.tables.update({name: Table(name=name, column_names=column_names.split(','), column_types=column_types.split(','), primary_key=primary_key, load=load)})
        # self._name = Table(name=name, column_names=column_names, column_types=column_types, load=load)
        # check that new dynamic var doesnt exist already
        # self.no_of_tables += 1
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


    def import_table(self, table_name, filename, column_types=None, primary_key=None):
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
                self.create_table(name=table_name, column_names=colnames, column_types=column_types, primary_key=primary_key)
                lock_ownership = self.lock_table(table_name, mode='x')
                first_line = False
                continue
            self.tables[table_name]._insert(line.strip('\n').split(','))

        if lock_ownership and not self.save_state:
            self.unlock_table(table_name)
        else:
            self.transaction_locks+=table_name
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
        row = row_str.strip().split(',')
        self.load_database()
        # call returns a dictionary with the triggers that need to be excecuted(probably wrong grammatically)
        trigger = self._trigger_select(table_name,'insert')
        #returns list of triggers that need to run in before
        #if it is empty we dont go in
        if trigger.get('before'):
            execute = trigger.get('before')
            for functions in execute:
                getattr(__import__(functions[0]), functions[1])(self.tables[table_name])
        #same logic but insert dont run
        if trigger.get('instead'):
            execute = trigger.get('instead')
            print(execute)
            for functions in execute:
                print('got here')
                getattr(__import__(functions[0]), functions[1])(self.tables[table_name])
        else:
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
            self._update_meta_insert_stack_for_tb(table_name, insert_stack[:-1])
            if lock_ownership and not self.save_state:
                self.unlock_table(table_name)
            else:
                self.transaction_locks += table_name
            self._update()
            self.save_database()
            if trigger.get('after'):
                execute = trigger.get('after')
                for functions in execute:
                    getattr(__import__(functions[0]), functions[1])(self.tables[table_name])


    # read comments in delete or insert same logic this is not an agatha christie novel
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
        set_column, set_value = set_args.replace(' ','').split('=')
        self.load_database()
        trigger=self._trigger_select(table_name,'delete')
        if trigger.get('before'):
            execute = trigger.get('before')
            for functions in execute:
                getattr(__import__(functions[0]), functions[1])(self.tables[table_name])
        if trigger.get('instead'):
            execute = trigger.get('instead')
            for functions in execute:
                getattr(__import__(functions[0]), functions[1])(self.tables[table_name])
        else:
            lock_ownership = self.lock_table(table_name, mode='x')
            self.tables[table_name]._update_rows(set_value, set_column, condition)
            if lock_ownership and not self.save_state:
                self.unlock_table(table_name)
            else:
                self.transaction_locks += table_name
            self._update()
            self.save_database()
        if trigger.get('after'):
            execute = trigger.get('after')
            for functions in execute:
                getattr(__import__(functions[0]), functions[1])(self.tables[table_name])


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
        # call returns a dictionary with the triggers that need to be excecuted(probably wrong grammar)
        trigger= self._trigger_select(table_name,'delete')
        if trigger.get('before'):
            #returns list of triggers that need to run in before
            #if it is empty we dont go in
            execute=trigger.get('before')
            for functions in execute:
                # runs aformentioned triggers
                getattr(__import__(functions[0]),functions[1])(self.tables[table_name])
        #same logic but delete dont run
        if trigger.get('instead'):
            execute = trigger.get('instead')
            print(execute)
            for functions in execute:
                print(functions)
                getattr(__import__(functions[0]), functions[1])(self.tables[table_name])
        
        else:
            lock_ownership = self.lock_table(table_name, mode='x')
            deleted = self.tables[table_name]._delete_where(condition)
            # dont unlock table if transaction has begun
            if lock_ownership and not self.save_state:
                self.unlock_table(table_name)
            # if transaction has begun add to list with tables to be unlocked
            else:
                self.transaction_locks.append(table_name)
            self._update()
            self.save_database()
            if table_name[:4] != 'meta':
                self._add_to_insert_stack(table_name, deleted)
            self.save_database()
        #run triggers after excecution of query 
        if trigger.get('after'):
            execute = trigger.get('after')
            for functions in execute:
                getattr(__import__(functions[0]), functions[1])(self.tables[table_name])

    def select(self, columns, table_name, condition, order_by=None, top_k=True, \
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
            return_object:DB=smdb python3.9 mdb.py
 boolean. If True, the result will be a table object (useful for internal use - the result will be printed by default).
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
        if lock_ownership and not self.save_state:
            self.unlock_table(table_name)
        else:
            self.transaction_locks+=table_name
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
                if not (os.path.isdir('/proc/{}'.format(pid))):
                     return False
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
    # starts transaction #82
    # anything run after this moment shall not be saved unless
    # commit command is run and saves it or
    # rollback in with phase we will roll to the last save 
    def start_transaction(self,action):
        # you cannot start a transaction whilst in a transaction is already running  
        if self.save_state:
            raise ValueError("Transaction already started")
        else:
            self.save_state=True

    # if transation begins and user want to undo what he has done( dont we all )
    # we turn savestate into false we call the unlocking phase well doing what its named
    # and we load the last save of players
    def rollback(self,action):

        if self.save_state:
            self.save_state=False
            self.unlocking_phase()
            self.load_database()
        # if a transaction has not started you cant really roll back can you
        else:
            raise ValueError("Transaction not started, nothng to rollback to")
    # if we wish to save the aformentioned transation via commit
    # we turn save state to False we unlock the locked tables and instead of loading last save
    # we save the altered (or perhaps not) DB as the now last save 
    def commit(self,action):
        if self.save_state:
            self.save_state=False
            self.unlocking_phase()
            self.save_database()
        # if a transaction has not started you cant really commit can you
        else:
            raise ValueError("Transaction not started, nothng to rollback to")

    # well it unlocks the locked tables
    def unlocking_phase(self):
        for table in self.transaction_locks:
            self.unlock_table(table)


    def create_trigger(self,params,table_name,function):
        
        self.load_database
        # intialize and error var in case of an error
        error= None
        # check if table exists or are we getting catfished
        if table_name in self.tables.keys():
            # check if timing is correct
            if params[1]=='before' or params[1]=='instead' or params[1]=='after':
                #check if procedure is implemented
                if params[2]=='update' or params[2]=='delete' or params[2]=='insert':
                    # add wanted trigger to the trigger table
                    self.tables['meta_triggers']._insert((table_name+','+params[0]+','+params[1]+','+params[2]+','+function).split(','))
                else:
                   error='Triggers only work on : Update,Delete,Insert'

            else:
                error='Trigger can only be executed on timings: Before,Instead,After'
        # if table doesnt exist
        else:
            error='There is no table '+table_name
        # if error has something print that something
        if error!=None:
            raise Exception(error)
        self.save_database()
        #[table_name]=[params[1],params[2],function,params[0]]

    def _trigger_select(self,table_name,where):
        # returns table and procedure triggers
        test=self.tables['meta_triggers']._select_where('*','act = '+where,)._select_where('*','table_name ='+table_name)
        # turn it into a list
        result_list=[[test.data[i][2]]+test.data[i][4:] for i in range(len(test.data))]
        # create a dict for insted before and after as keys
        result={result_list[i][0]:[] for i in range(len(result_list))}
        # initialize 3 empty lists gia ekasto shmeio
        instead=[]
        
        before=[]
        
        after=[]
        
        # add items to their predetermined lists
        for i in result_list:
            
            if i[0]=='instead':
                instead.append(i[1].split('.'))
            
            elif i[0]=='before':
                before.append([1].split('.'))
            
            else:
                after.append(i[1].split('.'))
        # add to dictionary each of their coresponding values
        result['instead']=instead
        
        result['after']=after
        
        result['before']=before
                
        return result

    def drop_trigger(self,trigger_name):
        self.load_database()
        self.tables['meta_triggers']._delete_where('trigger_name = '+trigger_name)
        self.save_database()