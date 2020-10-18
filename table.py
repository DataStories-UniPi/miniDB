from __future__ import annotations
from tabulate import tabulate
import pickle
import os
from misc import get_op, split_condition

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
    def __init__(self, name=None, column_names=None, column_types=None, primary_key=None, load=None):

        if load is not None:
            if isinstance(load, dict):
                self.__dict__.update(load)
                self._update()
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
            if primary_key is not None:
                self.pk_idx = self.column_names.index(primary_key)
            else:
                self.pk_idx = None


            self._update()


    def _update(self):
        self.columns = [[row[i] for row in self.data] for i in range(self._no_of_columns)]
        for ind, col in enumerate(self.column_names):
            setattr(self, col, self.columns[ind])

    def _cast_column(self, column_name, cast_type):
        column_idx = self.column_names.index(column_name)
        for i in range(len(self.data)):
            self.data[i][column_idx] = cast_type(self.data[i][column_idx])
        self.column_types[column_idx] = cast_type
        self._update()


    def _insert(self, row, insert_stack=[]):
        # row = row.split(',')

        if len(row)!=self._no_of_columns:
            raise ValueError(f'ERROR -> Cannot insert {len(row)} values. Only {self._no_of_columns} columns exist')

        for i in range(len(row)):
            try:
                row[i] = self.column_types[i](row[i])

            except:
                raise ValueError(f'ERROR -> Value {row[i]} is not of type {self.column_types[i]}.')

            if i==self.pk_idx and row[i] in self.columns[self.pk_idx]:
                # check pk
                raise ValueError(f'## ERROR -> Value {row[i]} already exists in primary key column.')

        if insert_stack != []:
            self.data[insert_stack[-1]] = row
        else:
            self.data.append(row)
        self._update()

    def _update_row(self, set_value, set_column, condition):
        column_name, operator, value = self._parse_condition(condition)

        column = self.columns[self.column_names.index(column_name)]
        set_column_idx = self.column_names.index(set_column)

        # set_columns_indx = [self.column_names.index(set_column_name) for set_column_name in set_column_names]

        for row_ind, column_value in enumerate(column):
            if value == column_value:
                self.data[row_ind][set_column_idx] = set_value

        self._update()
                # print(f"Updated {len(indexes_to_del)} rows")


    def _delete_where(self, condition):
        column_name, operator, value = self._parse_condition(condition)

        indexes_to_del = []

        column = self.columns[self.column_names.index(column_name)]
        for index, row_value in enumerate(column):
            if get_op(operator, row_value, value):
                indexes_to_del.append(index)

        # we pop from highest to lowest index in order to avoid removing the wrong item
        for index in sorted(indexes_to_del, reverse=True):
            print('del', index)
            if self.name[:4] != 'meta':
                self.data[index] = [None for _ in range(len(self.column_names))]
            else:
                self.data.pop(index)

        self._update()
        print(f"Deleted {len(indexes_to_del)} rows")
        return indexes_to_del


    def _select_indx(self, rows):
        if not isinstance(rows, list):
            rows = [rows]
        dict = {(key):([self.data[i] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}
        return Table(load=dict)

    def _select_where(self, return_columns, condition=None, order_by=None, asc=False, top_k=None):

        if return_columns == '*':
            return_cols = [i for i in range(len(self.column_names))]
        else:
            return_cols = [self.column_names.index(colname) for colname in return_columns]


        if condition is not None:
            column_name, operator, value = self._parse_condition(condition)
            column = self.columns[self.column_names.index(column_name)]
            rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]

        else:
            rows = [i for i in range(len(self.columns[0]))]

        rows = rows[:top_k]
        # TODO: this needs to be dumbed down
        dict = {(key):([[self.data[i][j] for j in return_cols] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}

        dict['column_names'] = [self.column_names[i] for i in return_cols]
        dict['column_types']   = [self.column_types[i] for i in return_cols]
        dict['_no_of_columns'] = len(return_cols)

        if order_by is None:
            return Table(load=dict)
        else:
            return Table(load=dict).order_by(order_by, asc)

    def show(self, no_of_rows=None, is_locked=False):
        if is_locked:
            print(f"\n## {self.name} (locked) ##")
        else:
            print(f"\n## {self.name} ##")

        headers = [f'{col} ({tp.__name__})' for col, tp in zip(self.column_names, self.column_types)]
        if self.pk_idx is not None:
            headers[self.pk_idx] = headers[self.pk_idx]+' #PK#'
        print(tabulate(self.data[:no_of_rows], headers=headers))

    def _load_from_file(self, filename):
        f = open(filename, 'rb')
        tmp_dict = pickle.load(f)
        f.close()

        self.__dict__.update(tmp_dict)



    def order_by(self, column_name, asc=False):
        column = self.columns[self.column_names.index(column_name)]
        idx = sorted(range(len(column)), key=lambda k: column[k], reverse=not asc)
        # print(idx)
        dict = {(key):([self.data[i] for i in idx] if key=="data" else value) for key, value in self.__dict__.items()}
        return Table(load=dict)


    def _sort(self, column_name, asc=False):
        column = self.columns[self.column_names.index(column_name)]
        idx = sorted(range(len(column)), key=lambda k: column[k], reverse=not asc)
        # print(idx)
        self.data = [self.data[i] for i in idx]
        self._update()

    def _inner_join(self, table_right: Table, condition):
        column_name_left, operator, column_name_right = self._parse_condition(condition, both_columns=True)
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

        no_of_ops = 0
        # this code is dumb on purpose... it needs to illustrate the underline technique
        for row_left in self.data:
            left_value = row_left[column_index_left]
            for row_right in table_right.data:
                right_value = row_right[column_index_right]
                no_of_ops+=1
                if get_op(operator, left_value, right_value): #EQ_OP
                    join_table._insert(row_left+row_right)

        print(f'## Select ops -> {no_of_ops}')
        print(f'# Left table size -> {len(self.data)}')
        print(f'# Right table size -> {len(table_right.data)}')

        return join_table



    def _parse_condition(self, condition, both_columns=False):
        if both_columns:
            return split_condition(condition)
        left, op, right = split_condition(condition)
        if left in self.column_names:
            column_name = left
            coltype = self.column_types[self.column_names.index(column_name)]
            value = coltype(right)
        elif right in self.column_names:
            column_name = right
            coltype = self.column_types[self.column_names.index(column_name)]
            value = coltype(left)
        else:
            raise ValueError(f'Condition is not valid (cant find column name)')
            return

        return column_name, op, value
