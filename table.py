from tabulate import tabulate
from __future__ import annotations
import os
from misc import get_op

class Table:
    '''
    Table object represents a table inside a database

    A Table object can be created either by assigning:
        - a table name (string)
        - column names (list of strings)
        - column types (list of functions like str/int etc)

    OR

        - by assigning a value to the variable called load. This value can be:
            - a path to a Table file saved using the save function
            - a dictionary that includes the appropriate info (all the attributes in __init__)

    '''
    def __init__(self, name=None, column_names=None, column_types=None, load=None):

        if load is not None:
            if isinstance(load, dict):
                self.__dict__.update(load)
            elif isinstance(load, str):
                self._load_from_file(load)

        elif (name is not None) and (column_names is not None) and (column_types is not None):

            self.name = name

            if len(column_names)!=len(column_types):
                raise ValueError('Need same number of column names and types.')

            self.column_names = column_names

            self.columns = []

            for col in self.column_names:
                if col not in self.__dir__():
                    setattr(self, col, [])
                    self.columns.append([])
                else:
                    raise Exception(f'"{col}" attribute already exists in "{self.__class__.__name__} "class.')

            self.column_types = column_types
            self._no_of_columns = len(column_names)
            self.data = []
            self.path = f'{self.name}_tmp.pkl'

            self._update()

    def _update(self):
        self.columns = [[row[i] for row in self.data] for i in range(self._no_of_columns)]
        for ind, col in enumerate(self.column_names):
            setattr(self, col, self.columns[ind])

        with open(self.path, 'wb') as f:
            pickle.dump(self.__dict__, f)

    def cast_column(self, column_name, cast_type):
        column_idx = self.column_names.index(column_name)
        for i in range(len(self.data)):
            self.data[i][column_idx] = cast_type(self.data[i][column_idx])
        self.column_types[column_idx] = cast_type
        self._update()


    def insert(self, row):
        # row = row.split(',')
        if self._is_locked():
            print(f"!! Table '{self.name}' is currently locked")
            return

        self._lock()

        if len(row)!=self._no_of_columns:
            raise ValueError(f'ERROR -> Cannot insert {len(row)} values. Only {self._no_of_columns} columns exist')

        for i in range(len(row)):
            try:
                row[i] = self.column_types[i](row[i])
            except:
                raise ValueError(f'ERROR -> Value {row[i]} is not of type {self.column_types[i]}.')
                return
        self.data.append(row)
        self._update()
        self._unlock()

    def _delete(self, row_no):
        if self._is_locked():
            print(f"!! Table '{self.name}' is currently locked")
            return
        self._lock()

        self.data.pop(row_no)
        self._update()

        self._unlock()

    def delete_where(self, column_name, operator, value):
        if self._is_locked():
            print(f"!! Table '{self.name}' is currently locked")
            return
        self._lock()

        indexes_to_del = []

        column = self.columns[self.column_names.index(column_name)]
        for index, row_value in enumerate(column):
            if get_op(operator, row_value, value):
                indexes_to_del.append(index)

        # we pop from highest to lowest index in order to avoid removing the wrong item
        for index in sorted(indexes_to_del, reverse=True):
            self.data.pop(index)

        self._update()
        self._unlock()
        print(f"Deleted {len(indexes_to_del)} rows")


    def _select_indx(self, rows):
        if not isinstance(rows, list):
            rows = [rows]
        dict = {(key):([self.data[i] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}
        return Table(load=dict)

    def select_where(self, column_name, operator, value):
        # TODO: this needs to be dumbed down
        column = self.columns[self.column_names.index(column_name)]
        rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]

        dict = {(key):([self.data[i] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}
        return Table(load=dict)


    def show(self, no_of_rows=5):
        print(f"# {self.name} #\n")
        print(tabulate(self.data[:no_of_rows], headers=self.column_names))

    def save(self, filename):
        if filename.split('.')[-1] != 'pkl':
            raise ValueError(f'ERROR -> Savefile needs .pkl extention')

        self.path = filename
        with open(self.path, 'wb') as f:
            pickle.dump(self.__dict__, f)

    def _load_from_file(self, filename):
        f = open(filename, 'rb')
        tmp_dict = pickle.load(f)
        f.close()

        self.__dict__.update(tmp_dict)



    def order_by(self, column_name, desc=False):
        idx = sorted(range(len(column_name)), key=lambda k: column_name[k], reverse=not desc)
        self.data = [self.data[i] for i in idx]

    def natural_join(self, table_right: Table, column_name):
        try:
            column_index_left = self.column_names.index(column_name)
            column_index_right = table_right.column_names.index(column_name)
        except:
            raise Exception(f'Column "{column_name}" doesnt exist in both tables.')

        left_names = [f'{self.name}_{colname}' for colname in self.column_names]
        right_names = [f'{table_right.name}_{colname}' for colname in table_right.column_names]


        join_table_name = f'{self.name}_join_{table_right.name}'
        join_table_colnames = left_names+right_names[:column_index_right]+right_names[column_index_right+1:]
        join_table_coltypes = self.column_types+table_right.column_types[:column_index_right]+table_right.column_types[column_index_right+1:]
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)

        # this code is dumb on purpose... it needs to illustrate the underline technique
        for row_left in self.data:
            left_value = row_left[column_index_left]
            for row_right in table_right.data:
                right_value = row_right[column_index_right]
                if left_value == right_value: #EQ_OP
                    join_table.insert(row_left+row_right[:column_index_right]+row_right[column_index_right+1:])

        return join_table

    def comparison_join(self, table_right: Table, column_name_left, column_name_right, operator='=='):
        try:
            column_index_left = self.column_names.index(column_name_left)
            column_index_right = table_right.column_names.index(column_name_right)
        except:
            raise Exception(f'Columns dont exist in one or both tables.')

        left_names = [f'{self.name}_{colname}' for colname in self.column_names]
        right_names = [f'{table_right.name}_{colname}' for colname in table_right.column_names]


        join_table_name = f'{self.name}_join_{table_right.name}'
        join_table_colnames = left_names+right_names
        join_table_coltypes = self.column_types+table_right.column_types
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)

        # this code is dumb on purpose... it needs to illustrate the underline technique
        for row_left in self.data:
            left_value = row_left[column_index_left]
            for row_right in table_right.data:
                right_value = row_right[column_index_right]
                if get_op(operator, left_value, right_value): #EQ_OP
                    join_table.insert(row_left+row_right)

        return join_table

    def _lock(self):
        with open(f'{self.name}.lock', 'w'): pass

    def _unlock(self):
        os.remove(f'{self.name}.lock')

    def _is_locked(self):
        return os.path.exists(f'{self.name}.lock')

    def _reload(self):
        self._load_from_file(self.path)
