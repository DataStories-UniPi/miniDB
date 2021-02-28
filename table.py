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
        - primary (name of the primary key column)

    OR

        - by assigning a value to the variable called load. This value can be:
            - a path to a Table file saved using the save function
            - a dictionary that includes the appropriate info (all the attributes in __init__)

    '''
    def __init__(self, name=None, column_names=None, column_types=None, primary_key=None, load=None):

        if load is not None:
            # if load is a dict, replace the object dict with it (replaces the object with the specified one)
            if isinstance(load, dict):
                self.__dict__.update(load)
                self._update()
            # if load is str, load from a file
            elif isinstance(load, str):
                self._load_from_file(load)

        # if name, columns_names and column types are not none
        elif (name is not None) and (column_names is not None) and (column_types is not None):

            self._name = name

            if len(column_names)!=len(column_types):
                raise ValueError('Need same number of column names and types.')

            self.column_names = column_names

            self.columns = []

            for col in self.column_names:
                if col not in self.__dir__():
                    # this is used in order to be able to call a column using its name as an attribute.
                    # example: instead of table.columns['column_name'], we do table.column_name
                    setattr(self, col, [])
                    self.columns.append([])
                else:
                    raise Exception(f'"{col}" attribute already exists in "{self.__class__.__name__} "class.')

            self.column_types = column_types
            self._no_of_columns = len(column_names)
            self.data = [] # data is a list of lists, a list of rows that is.

            # if primary key is set, keep its index as an attribute
            if primary_key is not None:
                self.pk_idx = self.column_names.index(primary_key)
            else:
                self.pk_idx = None


            self._update()

    # if any of the name, columns_names and column types are none. return an empty table object


    def _update(self):
        '''
        Update all the available column with the appended rows.
        '''
        self.columns = [[row[i] for row in self.data] for i in range(self._no_of_columns)]
        for ind, col in enumerate(self.column_names):
            setattr(self, col, self.columns[ind])

    def _cast_column(self, column_name, cast_type):
        '''
        Cast all values of a column using a specified type.
        '''
        # get the column from its name
        column_idx = self.column_names.index(column_name)
        # for every column's value in each row, replace it with itself but casted as the specified type
        for i in range(len(self.data)):
            self.data[i][column_idx] = cast_type(self.data[i][column_idx])
        # change the type of the column
        self.column_types[column_idx] = cast_type
        self._update()


    def _insert(self, row, insert_stack=[]):
        '''
        Insert row to table
        '''
        if len(row)!=self._no_of_columns:
            raise ValueError(f'ERROR -> Cannot insert {len(row)} values. Only {self._no_of_columns} columns exist')

        for i in range(len(row)):
            # for each value, cast and replace it in row.
            try:
                row[i] = self.column_types[i](row[i])

            except:
                raise ValueError(f'ERROR -> Value {row[i]} is not of type {self.column_types[i]}.')

            # if value is to be appended to the primary_key column, check that it doesnt alrady exist (no duplicate primary keys)
            if i==self.pk_idx and row[i] in self.columns[self.pk_idx]:
                raise ValueError(f'## ERROR -> Value {row[i]} already exists in primary key column.')

        # if insert_stack is not empty, append to its last index
        if insert_stack != []:
            self.data[insert_stack[-1]] = row
        else: # else append to the end
            self.data.append(row)
        self._update()

    def _update_row(self, set_value, set_column, condition):
        '''
        update where Condition
        '''
        # parse the condition
        column_name, operator, value = self._parse_condition(condition)

        # get the condition and the set column
        column = self.columns[self.column_names.index(column_name)]
        set_column_idx = self.column_names.index(set_column)

        # set_columns_indx = [self.column_names.index(set_column_name) for set_column_name in set_column_names]

        # for each value in column, if condition, replace it with set_value
        for row_ind, column_value in enumerate(column):
            if get_op(operator, column_value, value):
                self.data[row_ind][set_column_idx] = set_value

        self._update()
                # print(f"Updated {len(indexes_to_del)} rows")


    def _delete_where(self, condition):
        '''
        Deletes rows where condition is met.
        Important: delete replaces the rows to be deleted with rows filled with Nones,
        These rows are then appened to the insert_stack
        '''
        column_name, operator, value = self._parse_condition(condition)

        indexes_to_del = []

        column = self.columns[self.column_names.index(column_name)]
        for index, row_value in enumerate(column):
            if get_op(operator, row_value, value):
                indexes_to_del.append(index)

        # we pop from highest to lowest index in order to avoid removing the wrong item
        # since we dont delete, we dont have to to pop in that order, but since delete is used
        # to delete from meta tables too, we still implement it.

        for index in sorted(indexes_to_del, reverse=True):
            if self._name[:4] != 'meta':
                # if the table is not a metatable, replace the row with a row of nones
                self.data[index] = [None for _ in range(len(self.column_names))]
            else:
                self.data.pop(index)

        self._update()
        print(f"Deleted {len(indexes_to_del)} rows")
        # we have to return the deleted indexes, since they will be appended to the insert_stack
        return indexes_to_del


    def _select_where(self, return_columns, condition=None, order_by=None, asc=False, top_k=None):
        '''
        Select and return a table containing specified columns and rows where condition is met
        '''

        # if * return all columns, else find the column indexes for the columns specified
        if return_columns == '*':
            return_cols = [i for i in range(len(self.column_names))]
        elif isinstance(return_columns, str):
            raise Exception(f'Return columns should be "*" or of type list. (the second parameter is return_columns not condition)')
        else:
            return_cols = [self.column_names.index(colname) for colname in return_columns]

        # if condition is None, return all rows
        # if not, return the rows with values where condition is met for value
        if condition is not None:
            column_name, operator, value = self._parse_condition(condition)
            column = self.columns[self.column_names.index(column_name)]
            rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
        else:
            rows = [i for i in range(len(self.columns[0]))]

        # top k rows
        rows = rows[:top_k]
        # copy the old dict, but only the rows and columns of data with index in rows/columns (the indexes that we want returned)
        dict = {(key):([[self.data[i][j] for j in return_cols] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}

        # we need to set the new column names/types and no of columns, since we might
        # only return some columns
        dict['column_names'] = [self.column_names[i] for i in return_cols]
        dict['column_types']   = [self.column_types[i] for i in return_cols]
        dict['_no_of_columns'] = len(return_cols)

        # order by the return table if specified
        if order_by is None:
            return Table(load=dict)
        else:
            return Table(load=dict).order_by(order_by, asc)


    def _select_where_with_btree(self, return_columns, bt, condition, order_by=None, asc=False, top_k=None):

        # if * return all columns, else find the column indexes for the columns specified
        if return_columns == '*':
            return_cols = [i for i in range(len(self.column_names))]
        else:
            return_cols = [self.column_names.index(colname) for colname in return_columns]


        column_name, operator, value = self._parse_condition(condition)

        print("1: ", type(value), " 2: ", self.column_types[self.column_names.index(column_name)])

        # if the column in condition is not a primary key, abort the select
        if column_name != self.column_names[self.pk_idx]:
            print('Column is not PK. Aborting')

        # here we run the same select twice, sequentially and using the btree.
        # we then check the results match and compare performance (number of operation)
        column = self.columns[self.column_names.index(column_name)]

        # sequential
        rows1 = []
        opsseq = 0
        for ind, x in enumerate(column):
            opsseq+=1
            if get_op(operator, x, value):
                rows1.append(ind)

        print(f'Without Btree -> {opsseq} comparison operations')
        # btree find
        rows = bt.find(operator, value)
        print('### Seq result ###')
        print(rows1)
        print('### Index result ###')
        print(rows)

        # same as simple select from now on
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


    def order_by(self, column_name, asc=False):
        '''
        Order by based on column
        '''
        # get column, sort values and return sorted indexes
        column = self.columns[self.column_names.index(column_name)]
        idx = sorted(range(len(column)), key=lambda k: column[k], reverse=not asc)
        # return table but arange data using idx list (sorted indexes)
        dict = {(key):([self.data[i] for i in idx] if key=="data" else value) for key, value in self.__dict__.items()}
        return Table(load=dict)


    def _sort(self, column_name, asc=False):
        '''
        Same as order by, but its persistant
        '''
        column = self.columns[self.column_names.index(column_name)]
        idx = sorted(range(len(column)), key=lambda k: column[k], reverse=not asc)
        # print(idx)
        self.data = [self.data[i] for i in idx]
        self._update()


    def _inner_join(self, table_right: Table, condition):
        '''
        Join table (left) with a supplied table (right) where condition is met.
        '''
        # get columns and operator
        column_name_left, operator, column_name_right = self._parse_condition(condition, join=True)
        # try to find both columns, if you fail raise error
        try:
            column_index_left = self.column_names.index(column_name_left)
            column_index_right = table_right.column_names.index(column_name_right)
        except:
            raise Exception(f'Columns dont exist in one or both tables.')

        # get the column names of both tables with the table name in front
        # ex. for left -> name becomes left_table_name_name etc
        left_names = [f'{self._name}_{colname}' for colname in self.column_names]
        right_names = [f'{table_right._name}_{colname}' for colname in table_right.column_names]

        # define the new tables name, its column names and types
        join_table_name = f'{self._name}_join_{table_right._name}'
        join_table_colnames = left_names+right_names
        join_table_coltypes = self.column_types+table_right.column_types
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)

        # count the number of operations (<,> etc)
        no_of_ops = 0
        # this code is dumb on purpose... it needs to illustrate the underline technique
        # for each value in left column and right column, if condition, append the corresponding row to the new table
        for row_left in self.data:
            left_value = row_left[column_index_left]
            for row_right in table_right.data:
                right_value = row_right[column_index_right]
                no_of_ops+=1
                if get_op(operator, left_value, right_value): #EQ_OP
                    join_table._insert(row_left+row_right)

        print(f'## Select ops no. -> {no_of_ops}')
        print(f'# Left table size -> {len(self.data)}')
        print(f'# Right table size -> {len(table_right.data)}')

        return join_table


    def show(self, no_of_rows=None, is_locked=False):
        '''
        Pretty print the table
        '''
        # if the table is locked, add locked keyword to title
        if is_locked:
            print(f"\n## {self._name} (locked) ##")
        else:
            print(f"\n## {self._name} ##")

        # headers -> "column name (column type)"
        headers = [f'{col} ({tp.__name__})' for col, tp in zip(self.column_names, self.column_types)]
        if self.pk_idx is not None:
            # table has a primary key, add PK next to the appropriate column
            headers[self.pk_idx] = headers[self.pk_idx]+' #PK#'
        # detect the rows that are no tfull of nones (these rows have been deleted)
        # if we dont skip these rows, the returning table has empty rows at the deleted positions
        non_none_rows = [row for row in self.data if any(row)]
        # print using tabulate
        print(tabulate(non_none_rows[:no_of_rows], headers=headers)+'\n')


    def _parse_condition(self, condition, join=False):
        '''
        Parse the single string condition and return column/s value and operator
        '''
        # if both_columns (used by the join function) return the names of the names of the columns (left first)
        if join:
            return split_condition(condition)

        # cast the value with the specified column's type and return the column name, the operator and the casted value
        left, op, right = split_condition(condition)
        if left not in self.column_names:
            raise ValueError(f'Condition is not valid (cant find column name)')
        coltype = self.column_types[self.column_names.index(left)]

        return left, op, coltype(right)


    def _load_from_file(self, filename):
        '''
        Load table from a pkl file (not used currently)
        '''
        f = open(filename, 'rb')
        tmp_dict = pickle.load(f)
        f.close()

        self.__dict__.update(tmp_dict)
