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
import triggers
import mdb as mdb


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
        self.create_table('meta_length', 'table_name,no_of_rows', 'str,int',['None','None'])    #pass 'None' column constraints to meta tables
        self.create_table('meta_locks', 'table_name,pid,mode', 'str,int,str',['None','None','None'])
        self.create_table('meta_insert_stack', 'table_name,indexes', 'str,list',['None','None'])
        self.create_table('meta_indexes', 'table_name,index_name', 'str,str',['None','None'])
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


    def create_table(self, name, column_names, column_types, column_constraints, primary_key=None, load=None):
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
        self.tables.update({name: Table(name=name, column_names=column_names.split(','), column_types=column_types.split(','), column_constraints=column_constraints, primary_key=primary_key, load=load)})
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

    def insert_into(self, table_name, row_str,run_triggers=True):
        '''
        Inserts data to given table.

        Args:
            table_name: string. Name of table (must be part of database).
            row: list. A list of values to be inserted (will be casted to a predifined type automatically).
            lock_load_save: boolean. If False, user needs to load, lock and save the states of the database (CAUTION). Useful for bulk-loading.
            run_triggers: boolean. If true, triggers are allowed to run.
        '''

        if run_triggers:    #check for instead of trigger
            table = self.tables[table_name]
            for t in table.triggers:
                if t['when']=='instead of' and 'insert' in t['action']:
                    func = t['function']
                    query = getattr(triggers,func)()

                    if query:   #if trigger returns a query, run it through the interpreter again

                        dic = mdb.interpret(query)
                        result = mdb.execute_dic(dic,False)
                        if isinstance(result,Table):
                            result.show()
                    return
        

        if run_triggers:    #if triggers are allowed to run
            self.execute_trigger(table_name,'before','insert')  #search for before insert triggers

        row = row_str.strip().split(',')
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

        if run_triggers:
            self.execute_trigger(table_name,'after','insert')   #search for after insert triggers


    def update_table(self, table_name, set_args, condition,run_triggers=True):
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
            run_triggers: boolean. If true, triggers are allowed to run.
        '''

        if run_triggers:    #check for instead of trigger
            table = self.tables[table_name]
            for t in table.triggers:
                if t['when']=='instead of' and 'update' in t['action']:
                    func = t['function']
                    query = getattr(triggers,func)()

                    if query:   #if trigger returns a query, run it through the interpreter again

                        dic = mdb.interpret(query)
                        result = mdb.execute_dic(dic,False)
                        if isinstance(result,Table):
                            result.show()
                    return

        if run_triggers:
            self.execute_trigger(table_name,'before','update')  #search for before update triggers

        set_column, set_value = set_args.replace(' ','').split('=')
        self.load_database()
        
        lock_ownership = self.lock_table(table_name, mode='x')
        self.tables[table_name]._update_rows(set_value, set_column, condition)
        if lock_ownership:
            self.unlock_table(table_name)
        self._update()
        self.save_database()

        if run_triggers:
            self.execute_trigger(table_name,'after','update')   #search for after update triggers

    def delete_from(self, table_name, condition,run_triggers=True):
        '''
        Delete rows of table where condition is met.

        Args:
            table_name: string. Name of table (must be part of database).
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.
                
                Operatores supported: (<,<=,==,>=,>)
            triggers: boolean. If true, triggers are allowed to run.
        '''

        if run_triggers:    #check for instead of trigger
            table = self.tables[table_name]
            for t in table.triggers:
                if t['when']=='instead of' and 'delete' in t['action']:
                    func = t['function']
                    query = getattr(triggers,func)()

                    if query:   #if trigger returns a query, run it through the interpreter again

                        dic = mdb.interpret(query)
                        result = mdb.execute_dic(dic,False)
                        if isinstance(result,Table):
                            result.show()
                    return

        if run_triggers:
            self.execute_trigger(table_name,'before','delete')  #search for before delete triggers

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

        if run_triggers:
            self.execute_trigger(table_name,'after','delete')   #search for after delete triggers

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

    def create_trigger(self, trigger_name, when, table_name, action, procedure ):
        '''
        Add a new trigger to the specified table.
        Args:
            trigger_name: name of the trigger.
            when: when the trigger should execute (before/after/instead of)
            action: action that causes the trigger to run (insert/update/delete)
            table_name: table associated with the trigger
            procedure: function to execute when the trigger runs
        '''
        self.load_database()
        if self.is_locked(table_name):  #if table is locked do nothing
            return

        if hasattr(triggers,procedure): #if the function exists in the triggers.py file
            for t in self.tables.values():  #else search for table and add trigger to list of triggers of that table
                if t._name == table_name:
                    t._add_trigger({'name': trigger_name,'when': when,'action': action,'function': procedure})
        else:
            raise Exception('Trigger creation failed. No such function found.') 
        self.save_database()

    def drop_trigger(self, trigger_name, table_name):
        '''
        Drop a trigger from a table.
        Args:
            trigger_name: name of the trigger.
            table_name: name of the table that the trigger belongs to.
        '''
        self.load_database()
        if self.is_locked(table_name):  #if table is locked do nothing
            return
        
        for t in self.tables.values():  #else find table and remove trigger 
            if t._name == table_name:
                t._drop_trigger(trigger_name)
        
        self.save_database()
        
    def execute_trigger(self,table_name,when,action):
        '''
        Run a trigger.
        Args:
            table_name: name of the table that the trigger belongs to.
            when: when the trigger should run (before/after/instead of)
            action: action associated with the trigger (insert/update/delete)
        '''
        table = self.tables[table_name] #get specified table
        
        for t in table.triggers:    #search triggers
            if t['when']==when and action in t['action']:
                func = t['function']    #function to be executed
                query = getattr(triggers,func)()

                if query:   #if trigger returns a query, run it through the interpreter again

                    dic = mdb.interpret(query)
                    result = mdb.execute_dic(dic,False)
                    if isinstance(result,Table):
                        result.show()

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

    def _construct_index(self, table_name, index_name, column_name):
        '''
        Construct a btree on a table and save.

        Args:
            table_name: string. Table name (must be part of database).
            index_name: string. Name of the created index.
            column_name: string. Name of the column to create the index on. If it is None, create index on primary key.
        '''
        bt = Btree(3) # 3 is arbitrary

        if not column_name: #if column is not specified, create index on PK
        # for each record in the primary key of the table, insert its value and index to the btree
        for idx, key in enumerate(self.tables[table_name].column_by_name(self.tables[table_name].pk)):
            bt.insert(key, idx)
        else:
            #for each record in the predefined column of the table, insert its value and index to the btree
            for idx, key in enumerate(self.tables[table_name].column_by_name(column_name)):
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