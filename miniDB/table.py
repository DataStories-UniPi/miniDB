from __future__ import annotations
from typing import List, Tuple
from tabulate import tabulate
import pickle
import os
import sys

sys.path.append(f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/miniDB')

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
    def __init__(self, name: str = None, column_names: List[str] = None, column_types: List[type] = None, primary_key: str = None, load = None) -> None:
        if load is not None:
            # if load is a dict, replace the object dict with it (replaces the object with the specified one)
            if isinstance(load, dict):
                self.__dict__.update(load)
            # if load is str, load from a file
            elif isinstance(load, str):
                self._load_from_file(load)

        # if name, columns_names and column types are not none
        elif (name is not None) and (column_names is not None) and (column_types is not None):
            self._name = name
            if len(column_names) != len(column_types):
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
            self.column_types = [eval(ct) if not isinstance(ct, type) else ct for ct in column_types]
            self.data = [] # data is a list of lists, a list of rows that is.

            # if primary key is set, keep its index as an attribute
            if primary_key is not None:
                self.pk_idx = self.column_names.index(primary_key)
            else:
                self.pk_idx = None
            self.pk = primary_key

    # if any of the name, columns_names and column types are none. return an empty table object
    def column_by_name(self, column_name: str) -> List:
        return [row[self.column_names.index(column_name)] for row in self.data]

    def _update(self) -> None:
        '''
        Update all the available columns with the appended rows.
        '''
        self.columns = [[row[i] for row in self.data] for i in range(len(self.column_names))]
        for ind, col in enumerate(self.column_names):
            setattr(self, col, self.columns[ind])

    def _cast_column(self, column_name: str, cast_type: type) -> None:
        '''
        Cast all values of a column using a specified type.
        Args:
            column_name: string. The column that will be casted.
            cast_type: type. Cast type
def is_prime(n):
    """Return True if a non-negative integer is prime."""
    if n <= 1:
        return False
    for i in range(2, int(n ** 0.5) + 1):
        if n % i == 0:
            return False
    return True

def prime_factorization(n):
    """Return a list of tuples representing the prime factorization of a positive integer."""
    if n < 1:
        raise ValueError("Input must be a positive integer.")
    factors = []
    for i in range(2, n + 1):
        if is_prime(i):
            count = 0
            while n % i == 0:
                count += 1
                n //= i
            if count > 0:
                factors.append((i, count))
        if n == 1:
            break
    return factors

def gcd(a, b):
    """Return the greatest common divisor of two non-negative integers."""
    while b:
        a, b = b, a % b
    return a

def lcm(a, b):
    """Return the least common multiple of two non-negative integers."""
    return a * b // gcd(a, b)

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
            column_name: string. Name of column.
            desc: boolean. If True, order_by will return results in descending order (False by default).
        '''
        column = [val if val is not None else 0 for val in self.column_by_name(column_name)]
        idx = sorted(range(len(column)), key=lambda k: column[k], reverse=desc)
        # print(idx)
        self.data = [self.data[i] for i in idx]
        # self._update()


    def _general_join_processing(self, table_right:Table, condition, join_type):
        '''
        Performs the processes all the join operations need (regardless of type) so that there is no code repetition.

        Args:
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.
                
                Operators supported: (<,<=,==,>=,>)
        '''
        # get columns and operator
        column_name_left, operator, column_name_right = self._parse_condition(condition, join=True)
        # try to find both columns, if you fail raise error

        if(operator != '=' and join_type in ['left','right','full']):
            class CustomFailException(Exception):
                pass
            raise CustomFailException('Outer Joins can only be used if the condition operator is "=".\n')

        try:
            column_index_left = self.column_names.index(column_name_left)
        except:
            raise Exception(f'Column "{column_name_left}" dont exist in left table. Valid columns: {self.column_names}.')

        try:
            column_index_right = table_right.column_names.index(column_name_right)
        except:
            raise Exception(f'Column "{column_name_right}" dont exist in right table. Valid columns: {table_right.column_names}.')

        # get the column names of both tables with the table name in front
        # ex. for left -> name becomes left_table_name_name etc
        left_names = [f'{self._name}.{colname}' if self._name!='' else colname for colname in self.column_names]
        right_names = [f'{table_right._name}.{colname}' if table_right._name!='' else colname for colname in table_right.column_names]

        # define the new tables name, its column names and types
        join_table_name = ''
        join_table_colnames = left_names+right_names
        join_table_coltypes = self.column_types+table_right.column_types
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)

        return join_table, column_index_left, column_index_right, operator


    def _inner_join(self, table_right: Table, condition):
        '''
        Join table (left) with a supplied table (right) where condition is met.

        Args:
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.
                
                Operators supported: (<,<=,==,>=,>)
        '''
        join_table, column_index_left, column_index_right, operator = self._general_join_processing(table_right, condition, 'inner')

        # count the number of operations (<,> etc)
        no_of_ops = 0
        # this code is dumb on purpose... it needs to illustrate the underline technique
        # for each value in left column and right column, if condition, append the corresponding row to the new table
        for row_left in self.data:
            left_value = row_left[column_index_left]
            for row_right in table_right.data:
                right_value = row_right[column_index_right]
                if(left_value is None and right_value is None):
                    continue
                no_of_ops+=1
                if get_op(operator, left_value, right_value): #EQ_OP
                    join_table._insert(row_left+row_right)

        return join_table
    
    def _left_join(self, table_right: Table, condition):
        '''
        Perform a left join on the table with the supplied table (right).

        Args:
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.
                
                Operators supported: (<,<=,==,>=,>)
        '''
        join_table, column_index_left, column_index_right, operator = self._general_join_processing(table_right, condition, 'left')

        right_column = table_right.column_by_name(table_right.column_names[column_index_right])
        right_table_row_length = len(table_right.column_names)

        for row_left in self.data:
            left_value = row_left[column_index_left]
            if left_value is None:
                continue
            elif left_value not in right_column:
                join_table._insert(row_left + right_table_row_length*["NULL"])
            else:
                for row_right in table_right.data:
                    right_value = row_right[column_index_right]
                    if left_value == right_value:
                        join_table._insert(row_left + row_right)

        return join_table

    def _right_join(self, table_right: Table, condition):
        '''
        Perform a right join on the table with the supplied table (right).

        Args:
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.
                
                Operators supported: (<,<=,==,>=,>)
        '''
        join_table, column_index_left, column_index_right, operator = self._general_join_processing(table_right, condition, 'right')

        left_column = self.column_by_name(self.column_names[column_index_left])
        left_table_row_length = len(self.column_names)

        for row_right in table_right.data:
            right_value = row_right[column_index_right]
            if right_value is None:
                continue
            elif right_value not in left_column:
                join_table._insert(left_table_row_length*["NULL"] + row_right)
            else:
                for row_left in self.data:
                    left_value = row_left[column_index_left]
                    if left_value == right_value:
                        join_table._insert(row_left + row_right)

        return join_table
    
    def _full_join(self, table_right: Table, condition):
        '''
        Perform a full join on the table with the supplied table (right).

        Args:
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.
                
                Operators supported: (<,<=,==,>=,>)
        '''
        join_table, column_index_left, column_index_right, operator = self._general_join_processing(table_right, condition, 'full')

        right_column = table_right.column_by_name(table_right.column_names[column_index_right])
        left_column = self.column_by_name(self.column_names[column_index_left])

        right_table_row_length = len(table_right.column_names)
        left_table_row_length = len(self.column_names)
        
        for row_left in self.data:
            left_value = row_left[column_index_left]
            if left_value is None:
                continue
            if left_value not in right_column:
                join_table._insert(row_left + right_table_row_length*["NULL"])
            else:
                for row_right in table_right.data:
                    right_value = row_right[column_index_right]
                    if left_value == right_value:
                        join_table._insert(row_left + row_right)

        for row_right in table_right.data:
            right_value = row_right[column_index_right]

class Table:
    def __init__(self, name, column_names, column_types, pk_idx=None):
        self._name = name
        self.column_names = column_names
        self.column_types = column_types
        self.pk_idx = pk_idx
        self.data = []
        self.locked = False

    def insert(self, *values):
        if self.locked:
            raise Exception(f"Table is locked. Cannot insert into {self._name}.")
        if len(values) != len(self.column_names):
            raise ValueError(f"Number of values must match number of columns: {len(self.column_names)}")
        for i in range(len(values)):
            if values[i] is not None and not isinstance(values[i], self.column_types[i]):
                raise TypeError(f"Type of value {i+1} should be {self.column_types[i]}")
        self.data.append(values)

    def select(self, *columns, **conditions):
        distinct = conditions.get('distinct', False)

        # Get indices of columns to select
        if columns[0] == '*':
            indices = range(len(self.column_names))
        else:
            indices = [self.column_names.index(col) for col in columns]

        # Check conditions
        selected_rows = []
        for row in self.data:
            for col, op_val in conditions.items():
                op, val = split_condition(op_val)
                idx = self.column_names.index(col)
                if not compare(row[idx], op, val):
                    break
            else:
                # All conditions were met, so add the row
                selected_rows.append([row[i] for i in indices])

        # Sort and remove duplicates if necessary
        if distinct:
            selected_rows = sorted(selected_rows)
            selected_rows = [selected_rows[i] for i in range(len(selected_rows)) if i == 0 or selected_rows[i] != selected_rows[i-1]]

        # Return selected rows
        return selected_rows

    def update(self, col, new_value, **conditions):
        if self.locked:
            raise Exception(f"Table is locked. Cannot update {self._name}.")
        idx = self.column_names.index(col)
        for row in self.data:
            for c, op_val in conditions.items():
                op, val = split_condition(op_val)
                if not compare(row[self.column_names.index(c)], op, val):
                    break
            else:
                row[idx] = new_value

    def delete(self, **conditions):
        if self.locked:
            raise Exception(f"Table is locked. Cannot delete from {self._name}.")
        self.data = [row for row in self.data if not all(compare(row[self.column_names.index(c)], op, val) for c, op_val in conditions.items() for op, val in [split_condition(op_val)])]

    def join(self, other_table, left_column, right_column):
        join_table = Table(f"{self._name}_{other_table._name}", self.column_names+other_table.column_names, self.column_types+other_table.column_types)
        left_table_row_length = len(self.column_names)

        # join data
        for row_left in self.data:
            left_value = row_left[self.column_names.index(left_column)]
            for row_right in other_table.data:
                right_value = row_right[other_table.column_names.index(right_column)]
                    def update(self, col, new_value, **conditions):
        if self.locked:
            raise Exception(f"Table is locked. Cannot update {self._name}.")
        idx = self.column_names.index(col)
        for row in self.data:
            for c, op_val in conditions.items():
                op, val = split_condition(op_val)
                if not compare(row[self.column_names.index(c)], op, val):
                    break
            else:
                row[idx] = new_value
