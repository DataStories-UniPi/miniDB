from __future__ import annotations
import pickle
from table import Table
from time import sleep, localtime, strftime
import os

class Database:
    '''
    Database class contains tables.
    '''

    def __init__(self, name, load=True):
        self.tables = {}
        self.name = name

        self.savedir = f'dbdata/{name}_db'

        if load:
            try:
                self.load(self.savedir)
                print(f'"{name}" loaded')
                return
            except:
                print(f"'{name}' db does not exist, creating new.")
                pass

        if not os.path.exists('dbdata'):
            os.mkdir('dbdata')
        # replacing doesnt work
        os.mkdir(self.savedir)

        self.create_table('meta_length',  ['table_name', 'no_of_rows'], [str, int])
        self.create_table('meta_locks',  ['table_name', 'locked'], [str, str])

        ## saves ##
        # self.meta.update({'length': Table('length', ['table_name', 'no_of_rows'], [str, int])}

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
        for file in os.listdir(path):
            f = open(path+'/'+file, 'rb')
            tmp_dict = pickle.load(f)
            f.close()
            name = f'{file.split(".")[0]}'
            self.tables.update({name: tmp_dict})
            setattr(self, name, self.tables[name])

    #### IO ####

    def _update(self):
        '''
        recalculate the number of tables in the Database
        '''
        self._update_meta_length()
        self._update_meta_locks()


    def create_table(self, name=None, column_names=None, column_types=None, load=None):
        '''
        This method create a new table. This table is saved and can be accessed by
        db_object.tables['table_name']
        or
        db_object.table_name
        '''
        self.tables.update({name: Table(name=name, column_names=column_names, column_types=column_types, load=load)})
        # self.name = Table(name=name, column_names=column_names, column_types=column_types, load=load)
        # check that new dynamic var doesnt exist already
        if name not in self.__dir__():
            setattr(self, name, self.tables[name])
        else:
            raise Exception(f'"{name}" attribute already exists in "{self.__class__.__name__} "class.')
        # self.no_of_tables += 1
        # self._update()


    def drop_table(self, table_name):
        '''
        Drop table with name 'table_name' from current db
        '''
        if self.tables[table_name]._is_locked():
            print(f"!! Table '{table_name}' is currently locked")
            return

        del self.tables[table_name]
        delattr(self, table_name)
        self._update()


    def table_from_csv(self, filename, name=None, column_types=None):
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
                self.create_table(name=name, column_names=colnames, column_types=[str for _ in colnames])
                first_line = False
                continue

            self.insert(name, line.strip('\n').split(','))


    def table_from_pkl(self, path):
        '''
        Load a table from a pkl file. tmp tables and saved tables are all saved using the pkl extention.
        '''
        new_table = Table(load=path)

        if new_table.name not in self.__dir__():
            setattr(self, new_table.name, new_table)
        else:
            raise Exception(f'"{new_table.name}" attribute already exists in "{self.__class__.__name__} "class.')

        self.tables.update({new_table.name: new_table})
        self._update()


    ##### table functions #####

    def cast_column(self, table_name, column_name, cast_type):
        self.load(self.savedir)
        self.lockX_table(table_name)
        self.tables[table_name]._cast_column(column_name, cast_type)
        self.unlock_table(table_name)
        self._update()
        self.save()

    def insert(self, table_name, row):
        self.load(self.savedir)
        self.lockX_table(table_name)
        self.tables[table_name]._insert(row)
        # sleep(2)
        self.unlock_table(table_name)
        self._update()
        self.save()

    def update_row(self, table_name, set_value, set_column, column_name, operator, value):
        self.load(self.savedir)
        self.lockX_table(table_name)
        self.tables[table_name]._update_row(set_value, set_column, column_name, operator, value)
        self.unlock_table(table_name)
        self._update()
        self.save()

    def delete(self, table_name, column_name, operator, value):
        self.load(self.savedir)
        self.lockX_table(table_name)
        self.tables[table_name]._delete_where(column_name, operator, value)
        self.unlock_table(table_name)
        self._update()
        self.save()

    def select(self, table_name, columns, column_name, operator, value):
        self.load(self.savedir)
        if self.is_locked(table_name):
            print(f'"{table_name}" table is currently locked')
            return
        return self.tables[table_name]._select_where(columns, column_name, operator, value)

    def show_table(self, table_name, no_of_rows=None):
        self.load(self.savedir)
        if self.is_locked(table_name):
            print(f'"{table_name}" table is currently locked')
            return
        self.tables[table_name].show(no_of_rows, self.is_locked(table_name))

    def sort(self, table_name, column_name, desc=False):
        self.load(self.savedir)
        self.lockX_table(table_name)
        self.tables[table_name]._sort(column_name, desc=desc)
        self.unlock_table(table_name)
        self._update()
        self.save()

    def inner_join(self, left_table_name, right_table_name, column_name_left, column_name_right, operator='==', save_name=None):
        self.load(self.savedir)
        if self.is_locked(left_table_name) or self.is_locked(right_table_name):
            print(f'Table/Tables are currently locked')
            return
        return self.tables[left_table_name]._inner_join(self.tables[right_table_name], column_name_left, column_name_right, operator='==')

    def lockX_table(self, table_name):
        if self.is_locked(table_name):
            print(f'"{table_name}" table is currently locked')
            return
        if table_name[:4]=='meta':
            return
        self.tables['meta_locks']._update_row(True, 'locked', 'table_name', '==', table_name)
        self._save_locks()
        # print(f'Locking table "{table_name}"')

    def unlock_table(self, table_name):
        self.tables['meta_locks']._update_row(False, 'locked', 'table_name', '==', table_name)
        self._save_locks()
        # print(f'Unlocking table "{table_name}"')

    def is_locked(self, table_name):
        if table_name[:4]=='meta':
            return False
        with open(f'{self.savedir}/meta_locks.pkl', 'rb') as f:
            self.tables.update({'meta_locks': pickle.load(f)})
            self.meta_locks = self.tables['meta_locks']

        try:
            return self.select('meta_locks', ['locked'], 'table_name', '==', table_name).locked[0]
        except IndexError:
            return

    #### META ####

    def _update_meta_length(self):
        for table in self.tables.values():
            if table.name[:4]=='meta':
                continue
            if table.name not in self.meta_length.table_name:
                self.tables['meta_length']._insert([table.name, 0])

            self.tables['meta_length']._update_row(len(table.data), 'no_of_rows', 'table_name', '==', table.name)
            # self.update_row('meta_length', len(table.data), 'no_of_rows', 'table_name', '==', table.name)

    def _update_meta_locks(self):
        for table in self.tables.values():
            if table.name[:4]=='meta':
                continue
            if table.name not in self.meta_locks.table_name:

                self.tables['meta_locks']._insert([table.name, False])
                # self.insert('meta_locks', [table.name, False])
