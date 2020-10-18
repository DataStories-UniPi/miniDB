from __future__ import annotations
import pickle
from table import Table
from time import sleep, localtime, strftime
import os
from btree import Btree

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
        self.create_table('meta_locks',  ['table_name', 'locked'], [str, bool])
        self.create_table('meta_insert_stack',  ['table_name', 'indexes'], [str, list])
        self.create_table('meta_indexes',  ['table_name', 'index_name'], [str, str])
        self.save()

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
        self._update_insert_stack()


    def create_table(self, name=None, column_names=None, column_types=None, primary_key=None, load=None):
        '''
        This method create a new table. This table is saved and can be accessed by
        db_object.tables['table_name']
        or
        db_object.table_name
        '''
        self.tables.update({name: Table(name=name, column_names=column_names, column_types=column_types, primary_key=primary_key, load=load)})
        # self.name = Table(name=name, column_names=column_names, column_types=column_types, load=load)
        # check that new dynamic var doesnt exist already
        if name not in self.__dir__():
            setattr(self, name, self.tables[name])
        else:
            raise Exception(f'"{name}" attribute already exists in "{self.__class__.__name__} "class.')
        # self.no_of_tables += 1
        self._update()


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


    def table_from_object(self, new_table):
        '''
        Add table obj to database.
        '''

        self.tables.update({new_table.name: new_table})
        if new_table.name not in self.__dir__():
            setattr(self, new_table.name, new_table)
        else:
            raise Exception(f'"{new_table.name}" attribute already exists in "{self.__class__.__name__} "class.')
        self._update()
        self.save()



    ##### table functions #####

    def cast_column(self, table_name, column_name, cast_type):
        self.load(self.savedir)
        if self.is_locked(table_name):
            return
        self.lockX_table(table_name)
        self.tables[table_name]._cast_column(column_name, cast_type)
        self.unlock_table(table_name)
        self._update()
        self.save()

    def insert(self, table_name, row):
        self.load(self.savedir)
        if self.is_locked(table_name):
            return
        insert_stack = self._get_insert_stack_for_tb(table_name)
        self.lockX_table(table_name)
        try:
            self.tables[table_name]._insert(row, insert_stack)
        except Exception as e:
            print(e)
            print('ABORTED')
        # sleep(2)
        self.unlock_table(table_name)
        self._update()
        self.save()
        self._update_insert_stack_for_tb(table_name, insert_stack[:-1])
        self.save()


    def update_row(self, table_name, set_value, set_column, condition):
        self.load(self.savedir)
        if self.is_locked(table_name):
            return
        self.lockX_table(table_name)
        self.tables[table_name]._update_row(set_value, set_column, condition)
        self.unlock_table(table_name)
        self._update()
        self.save()

    def delete(self, table_name, condition):
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
        self.load(self.savedir)
        if self.is_locked(table_name):
            return

        if self._has_index(table_name):
            bt = self._load_idx()
            # TODO: add code here
        else:
            table = self.tables[table_name]._select_where(columns, condition, order_by, asc, top_k)

        if save_as is not None:
            table.name = save_as
            self.table_from_object(table)
        else:
            if return_object:
                return table
            else:
                table.show()

    def show_table(self, table_name, no_of_rows=None):
        self.load(self.savedir)
        if self.is_locked(table_name):
            return
        self.tables[table_name].show(no_of_rows, self.is_locked(table_name))

    def sort(self, table_name, column_name, asc=False):
        self.load(self.savedir)
        if self.is_locked(table_name):
            return
        self.lockX_table(table_name)
        self.tables[table_name]._sort(column_name, asc=asc)
        self.unlock_table(table_name)
        self._update()
        self.save()

    def inner_join(self, left_table_name, right_table_name, condition, save_as=None, return_object=False):
        self.load(self.savedir)
        if self.is_locked(left_table_name) or self.is_locked(right_table_name):
            print(f'Table/Tables are currently locked')
            return

        res = self.tables[left_table_name]._inner_join(self.tables[right_table_name], condition)
        if save_as is not None:
            res.name = save_as
            self.table_from_object(res)
        else:
            if return_object:
                return table
            else:
                table.show()

    def lockX_table(self, table_name):
        if table_name[:4]=='meta':
            return

        self.tables['meta_locks']._update_row(True, 'locked', f'table_name=={table_name}')
        self._save_locks()
        # print(f'Locking table "{table_name}"')

    def unlock_table(self, table_name):
        self.tables['meta_locks']._update_row(False, 'locked', f'table_name=={table_name}')
        self._save_locks()
        # print(f'Unlocking table "{table_name}"')

    def is_locked(self, table_name):
        if table_name[:4]=='meta':
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

    def _update_meta_length(self):
        for table in self.tables.values():
            if table.name[:4]=='meta':
                continue
            if table.name not in self.meta_length.table_name:
                self.tables['meta_length']._insert([table.name, 0])

            non_none_rows = len([row for row in table.data if any(row)])
            self.tables['meta_length']._update_row(non_none_rows, 'no_of_rows', f'table_name=={table.name}')
            # self.update_row('meta_length', len(table.data), 'no_of_rows', 'table_name', '==', table.name)

    def _update_meta_locks(self):
        for table in self.tables.values():
            if table.name[:4]=='meta':
                continue
            if table.name not in self.meta_locks.table_name:
                print(f'New table {table.name}')

                self.tables['meta_locks']._insert([table.name, False])
                # self.insert('meta_locks', [table.name, False])

    def _update_insert_stack(self):
        for table in self.tables.values():
            if table.name[:4]=='meta':
                continue
            if table.name not in self.meta_insert_stack.table_name:
                print('jey')
                self.tables['meta_insert_stack']._insert([table.name, []])


    def _add_to_insert_stack(self, table_name, indexes):
        old_lst = self._get_insert_stack_for_tb(table_name)
        self._update_insert_stack_for_tb(table_name, old_lst+indexes)

    def _get_insert_stack_for_tb(self, table_name):
        return self.select('meta_insert_stack', '*', f'table_name=={table_name}', return_object=True).indexes[0]

    def _update_insert_stack_for_tb(self, table_name, new_stack):
        return self.update_row('meta_insert_stack', new_stack, 'indexes', f'table_name=={table_name}')


    # indexes
    def create_index(self, table_name, index_name):
        if self.tables[table_name].pk_idx is None:
            print('## ERROR - Cant create index. Table has no primary key.')
            return
        if index_name not in self.tables['meta_indexes'].index_name:
            self.tables['meta_indexes']._insert([table_name, index_name])
            self._construct_index(table_name, index_name)
            self.save()
        else:
            print('## ERROR - Cant create index. Another index with the same name already exists.')
            return

    def _construct_index(self, table_name, index_name):
        bt = Btree(3)
        for idx, key in enumerate(self.tables[table_name].columns[self.tables[table_name].pk_idx]):
            bt.insert(key, idx)

        self._save_index(index_name, bt)


    def _has_index(self, table_name):
        return table_name in self.tables['meta_indexes'].table_name

    def _save_index(index_name, index):
        with open(f'{self.savedir}/{index_name}_index.pkl', 'wb') as f:
            pickle.dump(index, f)

    def _load_idx(index_name):
        f = open(f'{self.savedir}/{index_name}_index.pkl', 'rb')
        idx = pickle.load(f)
        f.close()
        return idx
