from __future__ import annotations
import pickle
from time import sleep, localtime, strftime
import os,sys
import logging
import warnings
import readline
import pandas as panda
from tabulate import tabulate
import re
sys.path.append(f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/miniDB')
from miniDB import table
sys.modules['table'] = table

from joins import Inlj, Smj
from btree import Btree
from misc import split_condition
from table import Table


# readline.clear_history()

class Database:
    '''
    Main Database class, containing tables.
    '''

    def __init__(self, name, load=True, verbose = True):
        self.tables = {}
        self._name = name
        self.verbose = verbose

        self.savedir = f'dbdata/{name}_db'

        if load:
            try:
                self.load_database()
                logging.info(f'Loaded "{name}".')
                return
            except:
                if verbose:
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
        '''
        if 'meta_index_type' not in self.tables:
            self.tables.update({name: Table(name=name, column_names=column_names, column_types=column_types, primary_key=primary_key, load=load)})
        '''
        self.tables.update({name: Table(name=name, column_names=column_names.split(','), column_types=column_types.split(','), primary_key=primary_key, load=load)})

        self._update()
        self.save_database()
        # (self.tables[name])

        if self.verbose:
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

        if self._has_index(table_name):
            to_be_deleted = []
            for key, table in enumerate(self.tables['meta_indexes'].column_by_name('table_name')):
                
                if table == table_name:
                    to_be_deleted.append(key)

            for i in reversed(to_be_deleted):
                self.drop_index(self.tables['meta_indexes'].data[i][1])

        try:
            delattr(self, table_name)
        except AttributeError:
            pass
        # self._update()
        
        self.save_database()


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

        #INTERVENTION TO CHECK IF TABLE TO BE DELETED HAS INSIDE IT A UNIQUE COLUMN 
        if os.path.isfile('./unique_table.pkl'):
                '''
                Here we check inside the unique_table.pkl and if there is a row with a table name that
                is equal to the table name of the table that is to be deleted , we delete the row 
                '''
                dataFR=panda.read_pickle('./unique_table.pkl')  
                #insert code that will delete the row where table_name = key in column named tab_name
                dataFR = dataFR[dataFR.tab_name != table_name]     
                dataFR.to_pickle('./unique_table.pkl')

        else:
            print('')
        
        #INTERVENTION TO CHECK IF TABLE TO BE DELETED IS INSIDE THE INDEX TYPE PKL FILE 
        if os.path.isfile('./meta_index_type.pkl'):
            '''
                Here we check inside the meta_index_type.pkl and if there is a row with a table name that
                is equal to the table name of the table that is to be deleted , we delete the row 
            '''
            dataFR1=panda.read_pickle('./meta_index_type.pkl') 
            dataFR1 = dataFR1[dataFR1.table_name != table_name]     
            dataFR1.to_pickle('./meta_index_type.pkl')  

            
        else:
            print('')


        #INTERVENTION TO CHECK IF TABLE TO BE DELETED HAS INDEX ON A UNIQUE COLUMN THEN WE DELETE DATA ROW OF PKL TABLE 
        if os.path.isfile('./index_uniques.pkl'):
            '''
                Here we check inside the index_uniques.pkl and if there is a row with a table name that
                is equal to the table name of the table that is to be deleted , we delete the row 
            '''
            dataFR2=panda.read_pickle('./index_uniques.pkl') 
            dataFR2 = dataFR2[dataFR2.table_name != table_name]     
            dataFR2.to_pickle('./index_uniques.pkl')  
       
        else:
            print('')
                
              
        if self._has_index(table_name):
            to_be_deleted = []
            for key, table in enumerate(self.tables['meta_indexes'].column_by_name('table_name')):
                if table == table_name:
                    to_be_deleted.append(key)

            for i in reversed(to_be_deleted):
                self.drop_index(self.tables['meta_indexes'].data[i][1])

        try:
            delattr(self, table_name)
        except AttributeError:
            pass
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

    def insert_into(self, table_name, row_str):
        '''
        Inserts data to given table.
        Args:
            table_name: string. Name of table (must be part of database).
            row: list. A list of values to be inserted (will be casted to a predifined type automatically).
            lock_load_save: boolean. If False, user needs to load, lock and save the states of the database (CAUTION). Useful for bulk-loading.
        '''
       
        row = row_str.strip().split(',')
        
        #INTERVENTION HERE WE CHECK IF TABLE HAS UNIQUE ROW AND IF YES THIS FUNCTION CHEKCS FOR SIMILAR DATA AND IF YES IT RAISES EXCEPTIONS 
        self._check_unique(row,table_name)

        self.load_database()
        # fetch the insert_stack. For more info on the insert_stack
        # check the insert_stack meta table
        lock_ownership = self.lock_table(table_name, mode='x')
        insert_stack = self._get_insert_stack_for_table(table_name)
       
        try:
            #self._check_unique(row,table_name)
            self.tables[table_name]._insert(row, insert_stack)
            
        except Exception as e:
            logging.info(e)
            logging.info('ABORTED')
        self._update_meta_insert_stack_for_tb(table_name, insert_stack[:-1])

        if lock_ownership:
            self.unlock_table(table_name)
        self._update()
        self.save_database()


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
        
        lock_ownership = self.lock_table(table_name, mode='x')
        self.tables[table_name]._update_rows(set_value, set_column, condition)
        if lock_ownership:
            self.unlock_table(table_name)
        self._update()
        self.save_database()

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
   
    def check_metaboy(self,table_name):
        '''
        Function used to check for data inside pkl table quicker and returns boolean value 
        '''
        if os.path.isfile('./unique_table.pkl'):
            
            dataFr1=panda.read_pickle('./unique_table.pkl')
           
            searcher=(dataFr1['tab_name']==table_name) 
            res2=dataFr1[searcher]
            if res2.empty:
                print('')
                return(res2.empty)
                    
            else:
                print(res2.empty)
                return(res2.empty)
        
    def select(self, columns, table_name, condition, distinct=None, order_by=None, \
               limit=True, desc=None, save_as=None, return_object=True):
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
            limit: int. An integer that defines the number of rows that will be returned (all rows if None).
            save_as: string. The name that will be used to save the resulting table into the database (no save if None).
            return_object: boolean. If True, the result will be a table object (useful for internal use - the result will be printed by default).
            distinct: boolean. If True, the resulting table will contain only unique rows.
        '''
       
        self.load_database()
        if isinstance(table_name,Table):
            
            return table_name._select_where(columns, condition, distinct, order_by, desc, limit)

        
        if condition is not None:
            condition_column = split_condition(condition)[0]
        else:
            condition_column = ''

      
        #INTERVENTION TO GET INDEX NAME IF TABLE WE ARE SELECTINF FROM , HAS ONE 
        index_name = self.get_index_name(table_name)
      

       
        #INTERVENTION HERE WE CHECK TO GET DATA FROM THE PKL TABLE CONTAINING INDEX TYPES 
        if os.path.isfile('./meta_index_type.pkl'):
            '''
            Checking for an index type if there is one coresponding to the table name we are checking now ,
            we append its value to the variable index_type 
            '''
            dataFR=panda.read_pickle('./meta_index_type.pkl')
            searcher=(dataFR['table_name']==table_name) 
            res=dataFR[searcher]
            if res.empty:
                print('')
                
            else:
                print(" ")
                unique_boy1=res.iloc[0]['index_type']

                index_type=unique_boy1
            
       
                    
  
        # self.lock_table(table_name, mode='x')
        if self.is_locked(table_name):
            print('')
            return
        
        
        table_chekpoint=table_name
        #INTERVENTION TO CHECK FOR FOR A UNIQUE COLUMN WITH INDEX ON IT ON THE UNIQUE TABLES 
        checker=self.check_metaboy(table_chekpoint)
        if os.path.isfile('./unique_table.pkl') and checker==False:
            
            dataFr1=panda.read_pickle('./unique_table.pkl')
            
            searcher=(dataFr1['tab_name']==table_chekpoint) 
            res22=dataFr1[searcher]
            if res22.empty:
                print('')
                    
            else:
                print("")
                    
                
            
        
            if os.path.isfile('./unique_table.pkl') and res22.empty==False:
                    
                        dataFr=panda.read_pickle('./unique_table.pkl')
                        print(table_chekpoint)
                        searcher=(dataFr['tab_name']==table_chekpoint) 
                        res1=dataFr[searcher]
                        if res1.empty:
                            print('')
                            
                        else:
                            print("")
                            
                        
                    
                            

                        if table_chekpoint in self._has_index(table_chekpoint) and res1.empty==False:
                                    '''
                                    Here we have reched the point of a search on a column that is unique and has b+tree index , then we 
                                    continue to execute the query with its arguments 
                                    '''
                                    if index_type == 'btree':
                                        print("Select using b+tree with a unique column")
                                        
                                        #index_name = self.select('*', 'meta_indexes', f'table_name={table_name}', return_object=True).column_by_name('index_name')[0]
                                        if condition_column=='':

                                            table = self.tables[table_name]._select_where(
                                            columns, condition, distinct, order_by, desc, limit)
                                        else:
                                            bt = self._load_idx(index_name)
                                            print(bt)
                                            table = self.tables[table_name]._select_where_with_btree(columns, bt, condition, distinct, order_by, desc, limit)
                        else:
                                table = self.tables[table_name]._select_where(columns, condition, distinct, order_by, desc, limit)
                                # self.unlock_table(table_name)
                                if save_as is not None:
                                    table._name = save_as
                                    self.table_from_object(table)
                                else:
                                    if return_object:
                                        return table
                                    else:
                                        return table.show()
        
        elif table_name in self._has_index(table_name) and condition_column == self.tables[table_name].column_names[self.tables[table_name].pk_idx]:
            

            if index_type == 'hash':
                    
                    '''
                    Here we have reached the point where we are searcing on the pk of table that has hash index on its pk column 
                    '''

                    indexer=self.tables['meta_indexes'].column_by_name('table_name').index(table_name)
                    index_name = self.tables['meta_indexes'].column_by_name('index_name')[indexer]
                    
                   
                    print('SELECT WHERE HASHINDEX ')
                    bt = self._load_idx(index_name)
                    

                    table = self.tables[table_name]._select_where_with_hashindex(columns, bt, condition, distinct, order_by, desc, limit)
            elif index_type == 'btree':
                '''
                Here we have reached the point where we are searcing on the pk of table that has b+tree index on its pk column 
                '''
                index_name = self.select(
                    '*', 'meta_indexes', f'table_name={table_name}', return_object=True).column_by_name('index_name')[0]
               
                bt = self._load_idx(index_name)

                table = self.tables[table_name]._select_where_with_btree(columns, bt, condition, distinct, order_by, desc, limit)
        #elif self._has_index(table_name) and condition_column == self.tables[table_name].column_names[self.tables[table_name].pk_idx]
        else:
            table = self.tables[table_name]._select_where(
                columns, condition, distinct, order_by, desc, limit)
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

    def create_view(self, table_name, table):
        '''
        Create a virtual table based on the result-set of the SQL statement provided.
        Args:
            table_name: string. Name of the table that will be saved.
            table: table. The table that will be saved.
        '''
        table._name = table_name
        self.table_from_object(table)

    def join(self, mode, left_table, right_table, condition, save_as=None, return_object=True):
        '''
        Join two tables that are part of the database where condition is met.
        Args:
            left_table: string. Name of the left table (must be in DB) or Table obj.
            right_table: string. Name of the right table (must be in DB) or Table obj.
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.
                
                Operators supported: (<,<=,==,>=,>)
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
        
        elif mode=='left':
            res = left_table._left_join(right_table, condition)
        
        elif mode=='right':
            res = left_table._right_join(right_table, condition)
        
        elif mode=='full':
            res = left_table._full_join(right_table, condition)

        elif mode=='inl':
            # Check if there is an index of either of the two tables available, as if there isn't we can't use inlj
            leftIndexExists = self._has_index(left_table._name)
            rightIndexExists = self._has_index(right_table._name)

            if not leftIndexExists and not rightIndexExists:
                res = None
                raise Exception('Index-nested-loop join cannot be executed. Use inner join instead.\n')
            elif rightIndexExists:
                index_name = self.select('*', 'meta_indexes', f'table_name={right_table._name}', return_object=True).column_by_name('index_name')[0]
                res = Inlj(condition, left_table, right_table, self._load_idx(index_name), 'right').join()
            elif leftIndexExists:
                index_name = self.select('*', 'meta_indexes', f'table_name={left_table._name}', return_object=True).column_by_name('index_name')[0]
                res = Inlj(condition, left_table, right_table, self._load_idx(index_name), 'left').join()

        elif mode=='sm':
            res = Smj(condition, left_table, right_table).join()

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




    def _check_unique(self, row, table_name):

        '''
        A function to check if there are any unique constraints in a table and ensure that the new data being inserted does not violate any of these constraints.
        this happens by checking the temporary data that has been inserted in the table , we extract the needed data , table name etc from the unique table.pkl 
        Args:
            row=row in def_insert into
            table_name= name of table data is to be inserted into 
        
        '''
    
        #current table columns
        columns_all_now = self.tables[table_name].column_names

        if os.path.isfile('./unique_table.pkl'):
            
            dataFr=panda.read_pickle('./unique_table.pkl')
            searcher=(dataFr['tab_name']==table_name) 
            res=dataFr[searcher]
            if res.empty:
                print('')
                
            else:
                print("Unique column found")
                unique_boy1=res.iloc[0]['unique_column']
            

            #the unique ones
            

                #the data
                data_now = self.tables[table_name].data

                for i in range (len(columns_all_now)):
                    if columns_all_now[i]==unique_boy1:
                        for f in range(len(data_now)):
                            for h in range(len(data_now[f])):
                                if row[i]==data_now[f][h]:
                                    raise Exception("error value exsists in unique column no insertion will be done ")
              

        
       







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
    def create_index(self, index_name, table_name, index_type):
       

        '''
        Creates an index on a specified table with a given name.
        Important: An index can only be created on a primary key (the user does not specify the column).
        Args:
            table_name: string. Table name (must be part of database).
            index_name: string. Name of the created index.
        '''
                  
        
        '''
        if 'meta_index_type' not in self.tables:
            self.create_table('meta_index_type', [
                              'table_name', 'index_name', 'index_type'], [str, str, str])
        '''








        #INTERVENTION , HERE WE BEGIN CHECKING IF THE INDEX WAS CREATED ON A UNIQUE COLUMN USING B TREE USING THE DATA OF THE INDEX UNIQUES THE IS CREATED WHEN SOMEONE
        #CREATE INDEX  INDEX_NAME ON TABLE_NAME(UNIQUE COLUMN NAME) USING BTREE
        if os.path.isfile('./index_uniques.pkl'):
            '''
            Here begins the creation or usage of a pkl file that has inside of it table names the unique table column and the name of the index 
            '''
            dataFR6=panda.read_pickle('./index_uniques.pkl')
            tab = dataFR6["table_name"].to_string(index=False)
            un_col = dataFR6["table_column"].to_string(index=False)
            index_n = dataFR6["index_name"].to_string(index=False)
            if os.path.isfile('./unique_table.pkl'):
                dataFr=panda.read_pickle('./unique_table.pkl')
                searcher=(dataFr['tab_name']==table_name.split(" ")[0]) 
                res=dataFr[searcher]
                
                if res.empty:
                    print('')
                    
                else:
                    unique_boy=res.iloc[0]['unique_column']
                    if tab==table_name.split(" ")[0] and un_col==unique_boy and index_name==index_n:
                        if index_name not in self.tables['meta_indexes'].column_by_name('index_name'):
                            if index_type == 'btree':
                                print('')
                                logging.info('Creating Btree index.')
                                
                                # insert a record with the name of the index and the table on which it's created to the meta_indexes table
                                self.tables['meta_indexes']._insert([table_name.split(" ")[0], index_name])

                                if os.path.isfile('./meta_index_type.pkl'):
                                        dataFR=panda.read_pickle('./meta_index_type.pkl')  

                                else:
                                    dataFR = panda.DataFrame(columns=['table_name', 'index_type','index_name'])
                                        
                                
                                dataFR=dataFR.append({'table_name': tab, 'index_type': index_type.lower(),'index_name':index_name},ignore_index=True)
                                dataFR.to_pickle('./meta_index_type.pkl')
                               


                                
                                
                                
                                
                                # crate the actual index
                                true = 1
                                self._construct_index_for_uniques_btree(self, table_name.split(" ")[0], index_name, index_type)

                                self.save_database()
                            return
                    else:
                        print('')
                        




        #INTERVENTION TO CHECK IF TABLE HAS UNIQUE COLUMN AT LEAST
        if os.path.isfile('./unique_table.pkl'):
            dataFr=panda.read_pickle('./unique_table.pkl')
            searcher=(dataFr['tab_name']==table_name) 
            res=dataFr[searcher]
            
            if res.empty:
                print('')
            else:
                unique_boy=res.iloc[0]['unique_column']
            

        
        if self.tables[table_name].pk_idx is None :  # if no primary key,
            raise Exception('Cannot create index. Table has no primary key or unique values.')
        
        
        
            

        #if self.tables[table_name].pk_idx is None and res.empty==False:
                
        if index_name not in self.tables['meta_indexes'].column_by_name('index_name'):
            if index_type == 'btree':
                logging.info('Creating Btree index.')
                
                # insert a record with the name of the index and the table on which it's created to the meta_indexes table
                self.tables['meta_indexes']._insert([table_name, index_name])
                #self.tables['meta_index_type']._insert([table_name, index_name, index_type.lower()])
                if os.path.isfile('./meta_index_type.pkl'):
                    dataFR=panda.read_pickle('./meta_index_type.pkl')  

                else:
                    dataFR = panda.DataFrame(columns=['table_name', 'index_type', 'index_name'])
                                        

                dataFR=dataFR.append({'table_name': table_name, 'index_type': index_type.lower(),'index_name':index_name},ignore_index=True)
                dataFR.to_pickle('./meta_index_type.pkl')

               
                
                # crate the actual index
               
                true = 1
                self._construct_index(self, table_name, index_name, index_type)
                

                print('index made')
                self.save_database()
                
                
            if index_type == 'hash':


            

                type = index_type

                logging.info('Creating hash index.')
                # insert a record with the name of the index and the table on which it's created to the meta_indexes table
                self.tables['meta_indexes']._insert([table_name, index_name])
                


                true = 1
                # crate the actual index
                
                self._construct_index(table_name, type, self, index_name)
                if os.path.isfile('./meta_index_type.pkl'):
                    dataFR=panda.read_pickle('./meta_index_type.pkl')  

                else:
                    dataFR = panda.DataFrame(columns=['table_name', 'index_type', 'index_name'])
                                        

                dataFR=dataFR.append({'table_name': table_name, 'index_type': index_type.lower(),'index_name':index_name},ignore_index=True)
                dataFR.to_pickle('./meta_index_type.pkl')

              
                self.save_database()
                print('Index made')
                
            return

        else:
            raise Exception(
                'Cannot create index. Another index with the same name already exists.')


    def _construct_index_for_uniques_btree(table_name, index_type, self, index_name, lsb=True):

        '''
        This function creates an index on unique column using btree , its diferent as the construct index in a way that it chekcks 
        data inside the unique_table.pkl ffile in order to extract the column to be indexed etc. 
         Args:
            table_name: string. Table name (must be part of database).
            index_name: string. Name of the created index.
        '''
      
        index_type1 = lsb
        index_name1 = lsb
        table_name1 = self
        self1 = index_type

        self = self1
        index_type = index_type1
        table_name = table_name1
        #index_name = index_name1
        
    

        if index_type == 'btree':
            bt = Btree(3)  # 3 is arbitrary
            


            if os.path.isfile('./unique_table.pkl'):
            
                dataFr=panda.read_pickle('./unique_table.pkl')
                searcher=(dataFr['tab_name']==table_name) 
                res=dataFr[searcher]
                if res.empty:
                    print('')
                    
                else:
                    
                    unique_boy1=res.iloc[0]['unique_column']
                
                   
            



            # for each record in the primary key of the table, insert its value and index to the btree
            for idx, key in enumerate(self.tables[table_name].column_by_name(unique_boy1)):
                if key is None:
                    continue
                bt.insert(key, idx)
            print('index made on unique column')
            # save the btree
            self._save_index(index_name, bt)









    def _construct_index(table_name, index_type, self, index_name, lsb=True):
        '''
        Construct a hash or btree on a table and save depending on index type.
        Args:
            table_name: string. Table name (must be part of database).
            index_name: string. Name of the created index.
            index_type: string. Type of index 
        '''

        #some confusions on the variables so i had to rearange
        index_type1 = lsb
        index_name1 = lsb
        table_name1 = self
        self1 = index_type

        self = self1
        index_type = index_type1
        table_name = table_name1
        #index_name = index_name1
    
        
        backup_table_name=self
        backup_index_name=index_type
        backup_index_type=table_name
        backup_self=index_name
        #print(backup_self.tables[backup_table_name].column_by_name(backup_self.tables[backup_table_name].pk))
        if backup_index_type == 'hash':  
            
            # create the index object
            index = HashIndex(depth=1, lsb=True)
            # insert records into the index
            for idx, key in enumerate(backup_self.tables[backup_table_name].column_by_name(backup_self.tables[backup_table_name].pk)):
                if key is None:
                    continue
                index.insert(key, idx)
            # use the search method to search for a specific key
            result = index.find(key)
            # use the delete method to delete a specific key
            index.delete(key)
            # use the get_all method to get all the key-value pairs in the index
            pairs = index.get_all()
            
            backup_self._save_index(backup_index_name, index)
            print('Index hash made')
           

        elif index_type == 'btree':
            bt = Btree(3)  # 3 is arbitrary

            
            # for each record in the primary key of the table, insert its value and index to the btree
            for idx, key in enumerate(self.tables[table_name].column_by_name(self.tables[table_name].pk)):
                if key is None:
                    continue
                bt.insert(key, idx)
            # save the btree
            
            self._save_index(index_name, bt)
            print('b+tree index made')
            


    def _has_index(self, table_name):
        '''
        Check whether the specified table's primary key column is indexed.
        Args:
            table_name: string. Table name (must be part of database).
        '''

        return self.tables['meta_indexes'].column_by_name('table_name')

    def _save_index(self, index_name, index):
        '''
        Save the index object.
        A  rgs:
            index_name: string. Name of the created index.
            index: obj. The actual index object (btree object or HashIndex object).
        '''
      

        try:
            os.mkdir(f'{self.savedir}/indexes')
        except:
            pass

        # check if the index is a HashIndex object
        if isinstance(index, HashIndex):
            # save the index as a pickle file
            with open(f'{self.savedir}/indexes/meta_{index_name}_index.pkl', 'wb') as f:
                pickle.dump(index, f)
        elif isinstance(index, Btree):
            # save the btree index as a pickle file
            with open(f'{self.savedir}/indexes/meta_{index_name}_index.pkl', 'wb') as f:
                pickle.dump(index, f)
        else:
            print('save error ')

    def get_index_name(self, table_name):
        meta_indexes = self.tables['meta_indexes']
        table_names = meta_indexes.column_by_name('table_name')
        index_names = meta_indexes.column_by_name('index_name')

        index_name = None
        if table_name in table_names:
            index_name = index_names[table_names.index(table_name)]

        return index_name

  

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

    def drop_index(self, index_name):
        '''
        Drop index from current database.
        Args:
            index_name: string. Name of index.
        '''
       
        #all the changes needed to delete all the index data from the pkl tabls  
        if os.path.isfile('./meta_index_type.pkl'):
            dataFR1=panda.read_pickle('./meta_index_type.pkl') 
            dataFR1 = dataFR1[dataFR1.index_name!= index_name]     
            dataFR1.to_pickle('./meta_index_type.pkl')

            print('')
        else:
            print('')
            
        

        if os.path.isfile('./index_uniques.pkl'):
            dataFR2=panda.read_pickle('./index_uniques.pkl') 
            dataFR2 = dataFR2[dataFR2.index_name != index_name]     
            dataFR2.to_pickle('./index_uniques.pkl')  
       
            print('')
        else:
            print('')
        if index_name in self.tables['meta_indexes'].column_by_name('index_name'):
            self.delete_from('meta_indexes', f'index_name = {index_name}')

            if os.path.isfile(f'{self.savedir}/indexes/meta_{index_name}_index.pkl'):
                os.remove(f'{self.savedir}/indexes/meta_{index_name}_index.pkl')
            else:
                warnings.warn(f'"{self.savedir}/indexes/meta_{index_name}_index.pkl" not found.')

            self.save_database()
        



class HashIndex:
    BUCKET_SIZE = 4
    HASH_SIZE = 64

    def __init__(self, depth=1, lsb=True):
        self.depth = depth
        self.lsb = lsb
        self.buckets = {}

    def insert(self, key, value):
        
        '''
        insert: takes a key and a value as input and hashes the 
        key using either the _hash_lsb or _hash_msb 
        function (depending on the value of lsb). 
        If the resulting hash value is already in the buckets 
        dictionary, it appends the key and value to the corresponding bucket.
        if not, it creates a new bucket with the given hash_value and 
        stores the key and value there. If the length of the bucket exceeds BUCKET_SIZE, 
        it calls the split_bucket function to split the bucket.
        '''
        
        if self.lsb:
            hash_value = self._hash_lsb(key)
        else:
            hash_value = self._hash_msb(key)

        if hash_value in self.buckets:
            self.buckets[hash_value].append((key, value))
        else:
            self.buckets[hash_value] = [(key, value)]
        if len(self.buckets[hash_value]) > self.BUCKET_SIZE:
            self.split_bucket(hash_value)

    def split_bucket(self, hash_value):

        '''
        split_bucket: takes a hash_value as input and splits the bucket 
        with the given hash_value. It increments the depth variable 
        and then rehashes all items in the old bucket using the updated depth value 
        '''
        self.depth += 1
        old_bucket = self.buckets[hash_value]
        del self.buckets[hash_value]
        for key, value in old_bucket:
            if self.lsb:
                new_hash_value = self._hash_lsb(key)
            else:
                new_hash_value = self._hash_msb(key)
            if new_hash_value in self.buckets:
                self.buckets[new_hash_value].append((key, value))
            else:
                self.buckets[new_hash_value] = [(key, value)]

    def _hash_lsb(self, key):
        '''
        _hash_lsb and _hash_msb: functions to hash the key using 
        either the least significant bits or most significant bits, respectively.
        '''
        return hash(key) % (2 ** self.depth)

    def _hash_msb(self, key):
        return hash(key) % (2 ** self.depth)

 

    def find(self, key):
        '''
        Description: This function looks up the value associated with a given key in a hash index. If the key is a two-tuple, perform a range query and return all values ​​between the start and end of the range. If the start or end is None, return all values ​​less than, greater than, or equal to the specified value respectively. Returns None if the key is not a tuple or does not match any value in the hash index.

            Args:

                key: The key to search for. Can be a tuple of two values ​​for range queries or a single value for exact matches.
                return the goods:

                If key is a tuple, returns a list of all values ​​between the start and end of the range, or all values ​​less than, greater than, or equal to the specified value, depending on whether start or end is None.
                If the key is a single value, returns the value associated with that key in the hash index, or None if the key is not found.
        '''
        if isinstance(key, tuple) and len(key) == 2:
            # range query
            start, end = key
            if start is None:
                # if start is None, return all values less than end
                result = []
                for bucket in self.buckets.values():
                    for k, v in bucket:
                        if k < end:
                            result.append(v)
                return result
            elif end is None:
                # if end is None, return all values greater than or equal to start
                result = []
                for bucket in self.buckets.values():
                    for k, v in bucket:
                        if k >= start:
                            result.append(v)
                return result
            else:
                # return all values between start and end
                result = []
                for bucket in self.buckets.values():
                    for k, v in bucket:
                        if start <= k < end:
                            result.append(v)
                return result
        else:
            # exact match
            if self.lsb:
                hash_value = self._hash_lsb(key)
            else:
                hash_value = self._hash_msb(key)

            if hash_value in self.buckets:
                for k, v in self.buckets[hash_value]:
                    if k == key:
                        return v
            return None

    def delete(self, key):
        '''
          delete: takes a key as input and deletes the pair with the given
          key from the hash index.
        '''
        if self.lsb:
            hash_value = self._hash_lsb(key)
        else:
            hash_value = self._hash_msb(key)

        if hash_value in self.buckets:
            self.buckets[hash_value] = [
                pair for pair in self.buckets[hash_value] if pair[0] != key]

    def get_all(self):
        '''
        get_all: returns a list of all key-value pairs in the hash index.




        '''
        pairs = []
        for bucket in self.buckets.values():
            pairs.extend(bucket)
        return pairs
