import os
import re
import sys
from collections import OrderedDict

class Database:
    def __init__(self):
        self.tables = {}

    def create_table(self, table_name, columns):
        if table_name in self.tables:
            print(f"Table {table_name} already exists")
            return

        self.tables[table_name] = Table(table_name, columns)

class Table:
    def __init__(self, table_name, columns):
        self.name = table_name
        self.columns = OrderedDict()
        self.indexes = {}

        for column in columns:
            column_name = column[0]
            column_type = column[1]

            if column_name in self.columns:
                print(f"Column {column_name} already exists in table {self.name}")
                return

            self.columns[column_name] = column_type

            if 'not null' in column:
                self.columns[column_name] += ' not null'

            if 'unique' in column:
                self.indexes[column_name] = Btree()

        print(f"Table {self.name} created with columns {self.columns}")

    def insert(self, values):
        if len(values) != len(self.columns):
            print("Number of values doesn't match number of columns")
            return

        for column_name, column_type in self.columns.items():
            if 'not null' in column_type and column_name not in values:
                print(f"Column {column_name} cannot be null")
                return

        for column_name in self.indexes:
            if values[column_name] in self.indexes[column_name]:
                print(f"Duplicate value found in column {column_name}")
                return
            self.indexes[column_name].add(values[column_name])

        print(f"Values {values} inserted into table {self.name}")

class Btree:
    def __init__(self):
        self.root = None

    def add(self, value):
        if not self.root:
            self.root = Node(value)
        else:
            self.root.add(value)

    def __contains__(self, value):
        if not self.root:
            return False
        else:
            return self.root.contains(value)

class Node:
    def __init__(self, value):
        self.value = value
        self.left = None
        self.right = None

    def add(self, value):
        if value < self.value:
            if self.left:
                self.left.add(value)
            else:
                self.left = Node(value)
        else:
            if self.right:
                self.right.add(value)
            else:
                self.right = Node(value)

    def contains(self, value):
        if value == self.value:
            return True
        elif value < self.value:
            if self.left:
                return self.left.contains(value)
            else:
                return False
        else:
            if self.right:
                return self.right.contains(value)
            else:
                return False

class Table:
    def __init__(self, name="", column_names=None, column_types=None, data=None):
        self._name = name
        self.column_names = column_names if column_names else []
        self.column_types = column_types if column_types else []
        self.data = data if data else []

    def _insert(self, row):
        assert len(row) == len(self.column_names), f"Row length {len(row)} does not match column length {len(self.column_names)}"
        self.data.append(row)

    def insert(self, row):
        if isinstance(row, dict):
            row = [row.get(col, None) for col in self.column_names]
        self._insert(row)

    def select(self, return_cols, where_cols, where_vals, limit=None, order_by=None, desc=False, distinct=False):
        '''
        Select data from table.

        Args:
            return_cols: list of strings. Names of columns to return.
            where_cols: list of strings. Names of columns in where clause.
            where_vals: list of strings. Values of columns in where clause.
            limit: int or str. Max number of rows to return.
            order_by: string. Column to order results by.
            desc: boolean. If True, order results in descending order.
            distinct: boolean. If True, remove duplicates from results.

        Returns:
            Table. Table with selected data.
        '''
        assert len(where_cols) == len(where_vals), "Number of where_cols and where_vals do not match"

        # create a list of booleans indicating if each row matches the where clause
        matches = [True] * len(self.data)
        for col, val in zip(where_cols, where_vals):
            col_idx = self.column_names.index(col)
            for i, row in enumerate(self.data):
                if val is None:
                    matches[i] = matches[i] and row[col_idx] is None
                else:
                    matches[i] = matches[i] and val == row[col_idx]

        # filter rows by the where clause
        rows = [i for i, match in enumerate(matches) if match]

        # get the maximum number of rows to return
        try:
            k = int(limit)
        except TypeError:
            k = None
        # same as simple select from now on
        rows = rows[:k]
        # TODO: this needs to be dumbed down
        dict = {(key):([[self.data[i][j] for j in return_cols] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}

        dict['column_names'] = [self.column_names[i] for i in return_cols]
        dict['column_types']   = [self.column_types[i] for i in return_cols]

        s_table = Table(load=dict)

        s_table.data = list(set(map(lambda x: tuple(x), s_table.data))) if distinct else s_table.data

        if order_by:
            s_table.order_by(order_by, desc)

        if isinstance(limit,str):
            s_table.data = [row for row in s_table.data if row is not None][:int(limit)]

        return s_table

    def order_by(self, column_name, desc=True):
        '''
        Order table based on column.

        Args:
            filename: string. Name of pkl file.
        '''
        f = open(filename, 'rb')
        tmp_dict = pickle.load(f)
        f.close()

        self.__dict__.update(tmp_dict.__dict__)
