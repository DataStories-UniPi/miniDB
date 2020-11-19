from __future__ import annotations
import pickle
from table import Table
from time import sleep, localtime, strftime
import os
from btree import Btree
import shutil

class Database:
    '''
    Database class contains tables.
    '''

    def __init__(self, name, load=True):
        self.tables = {}
        self._name = name

        self.savedir = f'dbdata/{name}_db'

        if load:
            try:
                self.load(self.savedir)
                print(f'"{name}" loaded')
                return
            except:
                print(f"'{name}' db does not exist, creating new.")
                pass

        # create dbdata directory if it doesnt exist
        if not os.path.exists('dbdata'):
            os.mkdir('dbdata')

        # create new dbs save directory
        try:
            os.mkdir(self.savedir)
        except:
            pass

        # create all the meta tables
        self.create_table('meta_length',  ['table_name', 'no_of_rows'], [str, int])
        self.create_table('meta_locks',  ['table_name', 'locked'], [str, bool])
        self.create_table('meta_insert_stack',  ['table_name', 'indexes'], [str, list])
        self.create_table('meta_indexes',  ['table_name', 'index_name'], [str, str])
        self.save()



    def save(self):
        '''
        Save db as a pkl file. This method saves the db object, ie all the tables and attributes.
        '''
        for name, table in self.tables.items():
            with open(f'{self.savedir}/{name}.pkl', 'wb') as f:
                pickle.dump(table, f)

    def _save_locks(self):
        '''
        Save db as a pkl file. This method saves the db object, ie all the tables and attributes.
        '''
        with open(f'{self.savedir}/meta_locks.pkl', 'wb') as f:
            pickle.dump(self.tables['meta_locks'], f)

    def load(self, path):
        '''
        Load all the tables that are part of the db (indexs are noted loaded here)
        '''
        for file in os.listdir(path):

            if file[-3:]!='pkl': # if used to load only pkl files
                continue
            f = open(path+'/'+file, 'rb')
            tmp_dict = pickle.load(f)
            f.close()
            name = f'{file.split(".")[0]}'
            self.tables.update({name: tmp_dict})
            setattr(self, name, self.tables[name])

    def drop_db(self):
        shutil.rmtree(self.savedir)

    #### IO ####

    def _update(self):
        '''
        Update all the meta tables.
        '''
        self._update_meta_length()
        self._update_meta_locks()
        self._update_meta_insert_stack()


    def create_table(self, name=None, column_names=None, column_types=None, primary_key=None, load=None):
        '''
        This method create a new table. This table is saved and can be accessed by
        db_object.tables['table_name']
        or
        db_object.table_name
        '''
        self.tables.update({name: Table(name=name, column_names=column_names, column_types=column_types, primary_key=primary_key, load=load)})
        # self._name = Table(name=name, column_names=column_names, column_types=column_types, load=load)
        # check that new dynamic var doesnt exist already
        if name not in self.__dir__():
            setattr(self, name, self.tables[name])
        else:
            raise Exception(f'"{name}" attribute already exists in "{self.__class__.__name__} "class.')
        # self.no_of_tables += 1
        print(f'New table {name}')
        self._update()
        self.save()


    def drop_table(self, table_name):
        '''
        Drop table with name 'table_name' from current db
        '''
        self.load(self.savedir)
        if self.is_locked(table_name):
            return

        self.tables.pop(table_name)
        delattr(self, table_name)
        os.remove(f'{self.savedir}/{table_name}.pkl')
        self.delete('meta_locks', f'table_name=={table_name}')
        self.delete('meta_length', f'table_name=={table_name}')
        self.delete('meta_insert_stack', f'table_name=={table_name}')

        # self._update()
        self.save()


    def table_from_csv(self, filename, name=None, column_types=None, primary_key=None):
        '''
        Create a table from a csv file.
        If name is not specified, filename's name is used
        If column types are not specified, all are regarded to be of type str
        '''
        if name is None:
            name=filename.split('.')[:-1][0]


        file = open(filename, 'r')

        first_line=True
        for line in file.readlines():
            if first_line:
                colnames = line.strip('\n').split(',')
                if column_types is None:
                    column_types = [str for _ in colnames]
                self.create_table(name=name, column_names=colnames, column_types=column_types, primary_key=primary_key)
                self.lockX_table(name)
                first_line = False
                continue
            self.tables[name]._insert(line.strip('\n').split(','))

        self.unlock_table(name)
        self._update()
        self.save()


    def table_to_csv(self, table_name, filename=None):
        res = ''
        for row in [self.tables[table_name].column_names]+self.tables[table_name].data:
            res+=str(row)[1:-1].replace("'", '').replace('"','').replace(' ','')+'\n'

        if filename is None:
            filename = f'{table_name}.csv'

        with open(filename, 'w') as file:
           file.write(res)

    def table_from_object(self, new_table):
        '''
        Add table obj to database.
        '''

        self.tables.update({new_table._name: new_table})
        if new_table._name not in self.__dir__():
            setattr(self, new_table._name, new_table)
        else:
            raise Exception(f'"{new_table._name}" attribute already exists in "{self.__class__.__name__} "class.')
        self._update()
        self.save()



    ##### table functions #####

    # In every table function a load command is executed to fetch the most recent table.
    # In every table function, we first check whether the table is locked. Since we have implemented
    # only the X lock, if the tables is locked we always abort.
    # After every table function, we update and save. Update updates all the meta tables and save saves all
    # tables.

    # these function calls are named close to the ones in postgres

    def cast_column(self, table_name, column_name, cast_type):
        '''
        Change the type of the specified column and cast all the prexisting values.
        Basically executes type(value) for every value in column and saves

        table_name -> table's name (needs to exist in database)
        column_name -> the column that will be casted (needs to exist in table)
        cast_type -> needs to be a python type like str int etc. NOT in ''
        '''
        self.load(self.savedir)
        if self.is_locked(table_name):
            return
        self.lockX_table(table_name)
        self.tables[table_name]._cast_column(column_name, cast_type)
        self.unlock_table(table_name)
        self._update()
        self.save()

    def insert(self, table_name, row, lock_load_save=True):
        '''
        Inserts into table

        table_name -> table's name (needs to exist in database)
        row -> a list of the values that are going to be inserted (will be automatically casted to predifined type)
        lock_load_save -> If false, user need to load, lock and save the states of the database (CAUTION). Usefull for bulk loading
        '''
        if lock_load_save:
            self.load(self.savedir)
            if self.is_locked(table_name):
                return
            # fetch the insert_stack. For more info on the insert_stack
            # check the insert_stack meta table
            self.lockX_table(table_name)
        insert_stack = self._get_insert_stack_for_table(table_name)
        try:
            self.tables[table_name]._insert(row, insert_stack)
        except Exception as e:
            print(e)
            print('ABORTED')
        # sleep(2)
        self._update_meta_insert_stack_for_tb(table_name, insert_stack[:-1])
        if lock_load_save:
            self.unlock_table(table_name)
            self._update()
            self.save()


    def update(self, table_name, set_value, set_column, condition):
        '''
        Update the value of a column where condition is met.

        table_name -> table's name (needs to exist in database)
        set_value -> the new value of the predifined column_name
        set_column -> the column that will be altered
        condition -> a condition using the following format :
                    'column[<,<=,==,>=,>]value' or
                    'value[<,<=,==,>=,>]column'.

                    operatores supported -> (<,<=,==,>=,>)
        '''
        self.load(self.savedir)
        if self.is_locked(table_name):
            return
        self.lockX_table(table_name)
        self.tables[table_name]._update_row(set_value, set_column, condition)
        self.unlock_table(table_name)
        self._update()
        self.save()

    def delete(self, table_name, condition):
        '''
        Delete rows of a table where condition is met.

        table_name -> table's name (needs to exist in database)
        condition -> a condition using the following format :
                    'column[<,<=,==,>=,>]value' or
                    'value[<,<=,==,>=,>]column'.

                    operatores supported -> (<,<=,==,>=,>)
        '''
        self.load(self.savedir)
        if self.is_locked(table_name):
            return
        self.lockX_table(table_name)
        deleted = self.tables[table_name]._delete_where(condition)
        self.unlock_table(table_name)
        self._update()
        self.save()
        # we need the save above to avoid loading the old database that still contains the deleted elements
        if table_name[:4]!='meta':
            self._add_to_insert_stack(table_name, deleted)
        self.save()

    def select(self, table_name, columns, condition=None, order_by=None, asc=False,\
               top_k=None, save_as=None, return_object=False):
        '''
        Selects and outputs a table's data where condtion is met.

        table_name -> table's name (needs to exist in database)
        columns -> The columns that will be part of the output table (use '*' to select all the available columns)
        condition -> a condition using the following format :
                    'column[<,<=,==,>=,>]value' or
                    'value[<,<=,==,>=,>]column'.

                    operatores supported -> (<,<=,==,>=,>)
        order_by -> A column name that signals that the resulting table should be ordered based on it. Def: None (no ordering)
        asc -> If True order by will return results using an ascending order. Def: False
        top_k -> A number (int) that defines the number of rows that will be returned. Def: None (all rows)
        save_as -> The name that will be used to save the resulting table in the database. Def: None (no save)
        return_object -> If true, the result will be a table object (usefull for internal usage). Def: False (the result will be printed)

        '''
        self.load(self.savedir)
        if self.is_locked(table_name):
            return
        self.lockX_table(table_name)
        if self._has_index(table_name) and condition is not None:
            index_name = self.select('meta_indexes', '*', f'table_name=={table_name}', return_object=True).index_name[0]
            bt = self._load_idx(index_name)
            table = self.tables[table_name]._select_where_with_btree(columns, bt, condition, order_by, asc, top_k)
        else:
            table = self.tables[table_name]._select_where(columns, condition, order_by, asc, top_k)
        self.unlock_table(table_name)
        if save_as is not None:
            table._name = save_as
            self.table_from_object(table)
        else:
            if return_object:
                return table
            else:
                table.show()

    def show_table(self, table_name, no_of_rows=None):
        '''
        Print a table using a nice tabular design (tabulate)

        table_name -> table's name (needs to exist in database)
        '''
        self.load(self.savedir)
        if self.is_locked(table_name):
            return
        self.tables[table_name].show(no_of_rows, self.is_locked(table_name))

    def sort(self, table_name, column_name, asc=False):
        '''
        Sorts a table based on a column

        table_name -> table's name (needs to exist in database)
        column_name -> the column that will be used to sort
        asc -> If True sort will return results using an ascending order. Def: False
        '''

        self.load(self.savedir)
        if self.is_locked(table_name):
            return
        self.lockX_table(table_name)
        self.tables[table_name]._sort(column_name, asc=asc)
        self.unlock_table(table_name)
        self._update()
        self.save()

    def inner_join(self, left_table_name, right_table_name, condition, save_as=None, return_object=False):
        '''
        Join two tables that are part of the database where condition is met.
        left_table_name -> left table's name (needs to exist in database)
        right_table_name -> right table's name (needs to exist in database)
        condition -> a condition using the following format :
                    'column[<,<=,==,>=,>]value' or
                    'value[<,<=,==,>=,>]column'.

                    operatores supported -> (<,<=,==,>=,>)
        save_as -> The name that will be used to save the resulting table in the database. Def: None (no save)
        return_object -> If true, the result will be a table object (usefull for internal usage). Def: False (the result will be printed)
        '''
        self.load(self.savedir)
        if self.is_locked(left_table_name) or self.is_locked(right_table_name):
            print(f'Table/Tables are currently locked')
            return

        res = self.tables[left_table_name]._inner_join(self.tables[right_table_name], condition)
        if save_as is not None:
            res._name = save_as
            self.table_from_object(res)
        else:
            if return_object:
                return res
            else:
                res.show()

    def lockX_table(self, table_name):
        '''
        Locks the specified table using the exclusive lock (X)

        table_name -> table's name (needs to exist in database)
        '''
        if table_name[:4]=='meta':
            return

        self.tables['meta_locks']._update_row(True, 'locked', f'table_name=={table_name}')
        self._save_locks()
        # print(f'Locking table "{table_name}"')

    def unlock_table(self, table_name):
        '''
        Unlocks the specified table that is exclusivelly locked (X)

        table_name -> table's name (needs to exist in database)
        '''
        self.tables['meta_locks']._update_row(False, 'locked', f'table_name=={table_name}')
        self._save_locks()
        # print(f'Unlocking table "{table_name}"')

    def is_locked(self, table_name):
        '''
        Check whether the specified table is exclusivelly locked (X)

        table_name -> table's name (needs to exist in database)
        '''
        if table_name[:4]=='meta':  # meta tables will never be locked (they are internal)
            return False

        with open(f'{self.savedir}/meta_locks.pkl', 'rb') as f:
            self.tables.update({'meta_locks': pickle.load(f)})
            self.meta_locks = self.tables['meta_locks']

        try:
            res = self.select('meta_locks', ['locked'], f'table_name=={table_name}', return_object=True).locked[0]
            if res:
                print(f'"{table_name}" table is currently locked')
            return res

        except IndexError:
            return

    #### META ####

    # The following functions are used to update, alter, load and save the meta tables.
    # Important: Meta tables contain info regarding the NON meta tables ONLY.
    # i.e. meta_length will not show the number of rows in meta_locks etc.

    def _update_meta_length(self):
        '''
        updates the meta_length table.
        '''
        for table in self.tables.values():
            if table._name[:4]=='meta': #skip meta tables
                continue
            if table._name not in self.meta_length.table_name: # if new table, add record with 0 no. of rows
                self.tables['meta_length']._insert([table._name, 0])

            # the result needs to represent the rows that contain data. Since we use an insert_stack
            # some rows are filled with Nones. We skip these rows.
            non_none_rows = len([row for row in table.data if any(row)])
            self.tables['meta_length']._update_row(non_none_rows, 'no_of_rows', f'table_name=={table._name}')
            # self.update_row('meta_length', len(table.data), 'no_of_rows', 'table_name', '==', table._name)

    def _update_meta_locks(self):
        '''
        updates the meta_locks table
        '''
        for table in self.tables.values():
            if table._name[:4]=='meta': #skip meta tables
                continue
            if table._name not in self.meta_locks.table_name:

                self.tables['meta_locks']._insert([table._name, False])
                # self.insert('meta_locks', [table._name, False])

    def _update_meta_insert_stack(self):
        '''
        updates the meta_insert_stack table
        '''
        for table in self.tables.values():
            if table._name[:4]=='meta': #skip meta tables
                continue
            if table._name not in self.meta_insert_stack.table_name:
                self.tables['meta_insert_stack']._insert([table._name, []])


    def _add_to_insert_stack(self, table_name, indexes):
        '''
        Added the supplied indexes to the insert stack of the specified table

        table_name -> table's name (needs to exist in database)
        indexes -> The list of indexes that will be added to the insert stack (the indexes of the newly deleted elements)
        '''
        old_lst = self._get_insert_stack_for_table(table_name)
        self._update_meta_insert_stack_for_tb(table_name, old_lst+indexes)

    def _get_insert_stack_for_table(self, table_name):
        '''
        Return the insert stack of the specified table

        table_name -> table's name (needs to exist in database)
        '''
        return self.tables['meta_insert_stack']._select_where('*', f'table_name=={table_name}').indexes[0]
        # res = self.select('meta_insert_stack', '*', f'table_name=={table_name}', return_object=True).indexes[0]
        # return res

    def _update_meta_insert_stack_for_tb(self, table_name, new_stack):
        '''
        Replaces the insert stack of a table with the one that will be supplied by the user

        table_name -> table's name (needs to exist in database)
        new_stack -> the stack that will be used to replace the existing one.
        '''
        self.tables['meta_insert_stack']._update_row(new_stack, 'indexes', f'table_name=={table_name}')


    # indexes
    def create_index(self, table_name, index_name, index_type='Btree'):
        '''
        Create an index on a specified table with a given name.
        Important: An index can only be created on a primary key. Thus the user does not specify the column

        table_name -> table's name (needs to exist in database)
        index_name -> name of the created index
        '''
        if self.tables[table_name].pk_idx is None: # if no primary key, no index
            print('## ERROR - Cant create index. Table has no primary key.')
            return
        if index_name not in self.tables['meta_indexes'].index_name:
            # currently only btree is supported. This can be changed by adding another if.
            if index_type=='Btree':
                print('Creating Btree index ')
                # insert a record with the name of the index and the table on which it's created to the meta_indexes table
                self.tables['meta_indexes']._insert([table_name, index_name])
                # crate the actual index
                self._construct_index(table_name, index_name)
                self.save()
        else:
            print('## ERROR - Cant create index. Another index with the same name already exists.')
            return

    def _construct_index(self, table_name, index_name):
        '''
        Construct a btree on a table and save.

        table_name -> table's name (needs to exist in database)
        index_name -> name of the created index
        '''
        bt = Btree(3) # 3 is arbitrary

        # for each record in the primary key of the table, insert its value and index to the btree
        for idx, key in enumerate(self.tables[table_name].columns[self.tables[table_name].pk_idx]):
            bt.insert(key, idx)
        # save the btree
        self._save_index(index_name, bt)


    def _has_index(self, table_name):
        '''
        Check whether the specified table's primary key column is indexed

        table_name -> table's name (needs to exist in database)
        '''
        return table_name in self.tables['meta_indexes'].table_name

    def _save_index(self, index_name, index):
        '''
        Save the index object

        index_name -> name of the created index
        index -> the actual index object (btree object)
        '''
        try:
            os.mkdir(f'{self.savedir}/indexes')
        except:
            pass

        with open(f'{self.savedir}/indexes/meta_{index_name}_index.pkl', 'wb') as f:
            pickle.dump(index, f)

    def _load_idx(self, index_name):
        '''
        load and return the specified index

        index_name -> name of the created index
        '''
        f = open(f'{self.savedir}/indexes/meta_{index_name}_index.pkl', 'rb')
        index = pickle.load(f)
        f.close()
        return index
