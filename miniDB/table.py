from __future__ import annotations
from tabulate import tabulate
import pickle
import os
import sys

sys.path.append(f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/miniDB')

from misc import *


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

    def __init__(self, name=None, column_names=None, column_types=None, unique=None, primary_key=None, load=None):
        if unique is not None: # Get the unique columns
            self.unique = unique
            self.unique_idx = []
            for i in range(len(unique)): # Get the indexes of the unique columns
                self.unique_idx.append(column_names.index(unique[i]))
        else: # If no unique columns
            self.unique = None
        if load is not None:
            # if load is a dict, replace the object dict with it (replaces the object with the specified one)
            if isinstance(load, dict):
                self.__dict__.update(load)
                # self._update()
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
            self.data = []  # data is a list of lists, a list of rows that is.

            # if primary key is set, keep its index as an attribute
            if primary_key is not None:
                self.pk_idx = self.column_names.index(primary_key)
            else:
                self.pk_idx = None

            self.pk = primary_key
            # self._update()

    # if any of the name, columns_names and column types are none. return an empty table object

    def column_by_name(self, column_name):
        return [row[self.column_names.index(column_name)] for row in self.data]

    def _update(self):
        '''
        Update all the available columns with the appended rows.
        '''
        self.columns = [[row[i] for row in self.data] for i in range(len(self.column_names))]
        for ind, col in enumerate(self.column_names):
            setattr(self, col, self.columns[ind])

    def _cast_column(self, column_name, cast_type):
        '''
        Cast all values of a column using a specified type.

        Args:
            column_name: string. The column that will be casted.
            cast_type: type. Cast type (do not encapsulate in quotes).
        '''
        # get the column from its name
        column_idx = self.column_names.index(column_name)
        # for every column's value in each row, replace it with itself but casted as the specified type
        for i in range(len(self.data)):
            self.data[i][column_idx] = cast_type(self.data[i][column_idx])
        # change the type of the column
        self.column_types[column_idx] = cast_type
        # self._update()

    def _insert(self, row, insert_stack=[]):
        '''
        Insert row to table.

        Args:
            row: list. A list of values to be inserted (will be casted to a predifined type automatically).
            insert_stack: list. The insert stack (empty by default).
        '''
        if len(row) != len(self.column_names):
            raise ValueError(f'ERROR -> Cannot insert {len(row)} values. Only {len(self.column_names)} columns exist')

        for i in range(len(row)):
            # for each value, cast and replace it in row.
            try:
                row[i] = self.column_types[i](row[i])
            except ValueError:
                if row[i] != 'NULL':
                    raise ValueError(
                        f'ERROR -> Value {row[i]} of type {type(row[i])} is not of type {self.column_types[i]}.')
            except TypeError as exc:
                if row[i] != None:
                    print(exc)

            if self.unique is not None: # Check that there are no duplicates in unique columns
                if i in self.unique_idx:
                    for j in range(len(self.unique_idx)): # Checks if value is already in the column
                        if i == self.unique_idx[j] and row[i] in self.column_by_name(self.unique[j]):
                            raise ValueError(f'## ERROR -> Value {row[i]} already exists in unique column.')

            # if value is to be appended to the primary_key column, check that it doesnt alrady exist (no duplicate primary keys)
            if i == self.pk_idx and row[i] in self.column_by_name(self.pk):
                raise ValueError(f'## ERROR -> Value {row[i]} already exists in primary key column.')
            elif i == self.pk_idx and row[i] is None:
                raise ValueError(f'ERROR -> The value of the primary key cannot be None.')

        # if insert_stack is not empty, append to its last index
        if insert_stack != []:
            self.data[insert_stack[-1]] = row
        else:  # else append to the end
            self.data.append(row)
        # self._update()

    def _update_rows(self, set_value, set_column, condition):
        '''
        Update where Condition is met.

        Args:
            set_value: string. The provided set value.
            set_column: string. The column to be altered.
            condition: string. A condition using the following format:
                'column[<,<=,=,>=,>]value' or
                'value[<,<=,=,>=,>]column'.
                
                Operatores supported: (<,<=,=,>=,>)
        '''
        # Get 'or' and 'and' sorted into lists so that conditions split by and could be executed together
        # This block of code is present in a lot of places
        _or = []
        _and = []
        if condition is not None:
            if ' or ' in condition: # Split or
                _or = condition.split(' or ')
                for i in _or:
                    if ' and ' in i:
                        _and.append(i.split(' and ')) # If and present in this or the conditions should be grouped
            elif ' and ' in condition: # If no ors split by and
                _and = [condition.split(' and ')]
            if ' and ' in condition: # Put between and together in _and
                for i in range(len(_and)):
                    end = len(_and[i]) # Length of the i condition in _and
                    j = 0
                    while j < end: # Check if between in _and item and put between and into one item
                        if 'between ' in _and[i][j]:
                            _and[i][j:j + 2] = [' and '.join(_and[i][j:j + 2])]
                            end -= 1
                        j += 1

        rows = [] # Rows to be updated (Used in 'and', 'or')
        if condition is not None and condition.count(' and ') == condition.count('between ') and not _or:
            if 'not between ' in condition:
                column_name, values = self._parse_condition(condition.replace('not ', '')) # Get column_name and the values to the right and left of 'and'
                column = self.column_by_name(column_name) # Get the column values
                set_column_idx = self.column_names.index(set_column) # The index of the column on which to update value

                for row_ind, column_value in enumerate(column): # Set the set_value where column_vaue is between values[0] and values[1]
                    if not get_op_between(column_value, values[0], values[1]):
                        self.data[row_ind][set_column_idx] = set_value
            elif 'between ' in condition:
                column_name, values = self._parse_condition(condition) # Same as above but 'not' doesn't need to be removed
                column = self.column_by_name(column_name)
                set_column_idx = self.column_names.index(set_column)

                for row_ind, column_value in enumerate(column):
                    if get_op_between(column_value, values[0], values[1]):
                        self.data[row_ind][set_column_idx] = set_value
            elif 'not ' in condition:
                # parse the condition
                column_name, operator, value = self._parse_condition(condition.replace('not ', '')) # Removing 'not' when parsing
                # get the condition and the set column
                column = self.column_by_name(column_name)
                set_column_idx = self.column_names.index(set_column)
                # set_columns_indx = [self.column_names.index(set_column_name) for set_column_name in set_column_names]

                # for each value in column, if condition, replace it with set_value
                for row_ind, column_value in enumerate(column):
                    if not get_op(operator, column_value, value):
                        self.data[row_ind][set_column_idx] = set_value
            else:
                # parse the condition
                column_name, operator, value = self._parse_condition(condition) # Same as above but 'not' doesn't need to be removed
                # get the condition and the set column
                column = self.column_by_name(column_name)
                set_column_idx = self.column_names.index(set_column)
                # set_columns_indx = [self.column_names.index(set_column_name) for set_column_name in set_column_names]

                # for each value in column, if condition, replace it with set_value
                for row_ind, column_value in enumerate(column):
                    if get_op(operator, column_value, value):
                        self.data[row_ind][set_column_idx] = set_value
        elif _and or _or: # if there are 'and' or 'or' connected conditions
            if _or and not _and: # If the conditions are connected by or and not and
                for i in _or: # Going through _or and getting the rows where the condition is True
                    if 'not ' in i: # If _and is empty there can't be between conditions because when making _and if there is a between there will be an and
                        column_name, operator, value = self._parse_condition(i.replace('not ', '')) # Same as above
                        column = self.column_by_name(column_name)
                        set_column_idx = self.column_names.index(set_column)

                        for row_ind, column_value in enumerate(column):
                            if not get_op(operator, column_value, value):
                                self.data[row_ind][set_column_idx] = set_value
                    else:
                        column_name, operator, value = self._parse_condition(i)
                        column = self.column_by_name(column_name)
                        set_column_idx = self.column_names.index(set_column)

                        for row_ind, column_value in enumerate(column):
                            if get_op(operator, column_value, value):
                                self.data[row_ind][set_column_idx] = set_value
            elif _and: # If there are only and connected conditions
                for j in _and:
                    and_rows = []
                    count = 0 # Counter so that the first rows will be set in and_rows with =
                    for i in j:
                        if 'not between ' in i: # Same as above but the rows are inserted into index_rows and in the end compared with the latest index_rows and only the same will be saved
                            column_name, values = self._parse_condition(i.replace('not ', ''))
                            column = self.column_by_name(column_name)
                            index_rows = [ind for ind, x in enumerate(column) if
                                          not get_op_between(x, values[0], values[1])]
                        elif 'between ' in i:
                            column_name, values = self._parse_condition(i)
                            column = self.column_by_name(column_name)
                            index_rows = [ind for ind, x in enumerate(column) if
                                          get_op_between(x, values[0], values[1])]
                        elif 'not ' in i:
                            column_name, operator, value = self._parse_condition(i.replace('not ', ''))
                            column = self.column_by_name(column_name)
                            index_rows = [ind for ind, x in enumerate(column) if not get_op(operator, x, value)]
                        else:
                            column_name, operator, value = self._parse_condition(i)
                            column = self.column_by_name(column_name)
                            index_rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
                        if count == 0:
                            and_rows = index_rows # and_rows becomes the first index_rows
                        else:
                            and_rows = set(and_rows).intersection(index_rows) # Get the same items of and_rows and index_rows
                        count += 1
                    rows += and_rows # After the loop rows becomes the and_rows
                if _or: # If or are present add the produced rows and delete duplicates
                    for i in _or:
                        if ' and ' not in i or i.count(' and ') == i.count('between '):
                            if 'not between ' in i: # Same as above
                                column_name, values = self._parse_condition(i.replace('not ', ''))
                                column = self.column_by_name(column_name)
                                index_rows = [ind for ind, x in enumerate(column) if
                                              not get_op_between(x, values[0], values[1])]
                            elif 'between ' in i:
                                column_name, values = self._parse_condition(i)
                                column = self.column_by_name(column_name)
                                index_rows = [ind for ind, x in enumerate(column) if
                                              get_op_between(x, values[0], values[1])]
                            elif 'not ' in i:
                                column_name, operator, value = self._parse_condition(i.replace('not ', ''))
                                column = self.column_by_name(column_name)
                                index_rows = [ind for ind, x in enumerate(column) if not get_op(operator, x, value)]
                            else:
                                column_name, operator, value = self._parse_condition(i)
                                column = self.column_by_name(column_name)
                                index_rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
                            rows += index_rows # In or connected conditions the rows are just added
            rows += list(set(rows)) # Removing duplicates
        else: # If no condition was given get all of the rows
            rows = [i for i in range(len(self.data))]
        if rows: # In case of _and or _or go through rows and update the rows
            set_column_idx = self.column_names.index(set_column)
            for row_ind in rows:
                self.data[row_ind][set_column_idx] = set_value

    def _delete_where(self, condition):
        '''
        Deletes rows where condition is met.

        Important: delete replaces the rows to be deleted with rows filled with Nones.
        These rows are then appended to the insert_stack.

        Args:
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.
                
                Operatores supported: (<,<=,==,>=,>)
        '''
        _or = [] # Same as above
        _and = []
        if condition is not None:
            if ' or ' in condition:
                _or = condition.split(' or ')
                for i in _or:
                    if ' and ' in i:
                        _and.append(i.split(' and '))
            elif ' and ' in condition:
                _and = [condition.split(' and ')]
            if ' and ' in condition:
                for i in range(len(_and)):
                    end = len(_and[i])
                    j = 0
                    while j < end:
                        if 'between ' in _and[i][j]:
                            _and[i][j:j + 2] = [' and '.join(_and[i][j:j + 2])]
                            end -= 1
                        j += 1

        indexes_to_del = [] # Indexes that will be deleted
        if condition is not None and condition.count(' and ') == condition.count('between ') and not _or: # If no ors, ther is a condition and no ands
            if 'not between ' in condition: # Same as above but we append to indexes_to_del
                column_name, values = self._parse_condition(condition.replace('not ', ''))
                column = self.column_by_name(column_name)

                for index, row_value in enumerate(column):
                    if not get_op_between(row_value, values[0], values[1]):
                        indexes_to_del.append(index)
            elif 'between ' in condition: # Same as above but we append to indexes_to_del
                column_name, values = self._parse_condition(condition.replace('not ', ''))
                column = self.column_by_name(column_name)

                for index, row_value in enumerate(column):
                    if get_op_between(row_value, values[0], values[1]):
                        indexes_to_del.append(index)
            elif 'not ' in condition: # Same as above but we append to indexes_to_del
                # parse the condition
                column_name, operator, value = self._parse_condition(condition.replace('not ', ''))
                # get the condition and the set column
                column = self.column_by_name(column_name)
                # set_columns_indx = [self.column_names.index(set_column_name) for set_column_name in set_column_names]

                # for each value in column, if condition, replace it with set_value
                for index, row_value in enumerate(column):
                    if not get_op(operator, row_value, value):
                        indexes_to_del.append(index)
            else: # Same as above but we append to indexes_to_del
                # parse the condition
                column_name, operator, value = self._parse_condition(condition)
                # get the condition and the set column
                column = self.column_by_name(column_name)
                # set_columns_indx = [self.column_names.index(set_column_name) for set_column_name in set_column_names]

                # for each value in column, if condition, replace it with set_value
                for index, row_value in enumerate(column):
                    if get_op(operator, row_value, value):
                        indexes_to_del.append(index)
        elif _and or _or: # Same as above but we append to indexes_to_del
            if _or and not _and:
                for i in _or:
                    if 'not ' in i:
                        column_name, operator, value = self._parse_condition(i.replace('not ', ''))
                        column = self.column_by_name(column_name)

                        for index, row_value in enumerate(column):
                            if not get_op(operator, row_value, value):
                                indexes_to_del.append(index)
                    else:
                        column_name, operator, value = self._parse_condition(i)
                        column = self.column_by_name(column_name)

                        for index, row_value in enumerate(column):
                            if get_op(operator, row_value, value):
                                indexes_to_del.append(index)
            elif _and: # Same as above but the produced rows go to indexes_to_del
                for j in _and:
                    and_rows = []
                    count = 0
                    for i in j:
                        if 'not between ' in i:
                            column_name, values = self._parse_condition(i.replace('not ', ''))
                            column = self.column_by_name(column_name)
                            index_rows = [ind for ind, x in enumerate(column) if
                                          not get_op_between(x, values[0], values[1])]
                        elif 'between ' in i:
                            column_name, values = self._parse_condition(i)
                            column = self.column_by_name(column_name)
                            index_rows = [ind for ind, x in enumerate(column) if
                                          get_op_between(x, values[0], values[1])]
                        elif 'not ' in i:
                            column_name, operator, value = self._parse_condition(i.replace('not ', ''))
                            column = self.column_by_name(column_name)
                            index_rows = [ind for ind, x in enumerate(column) if not get_op(operator, x, value)]
                        else:
                            column_name, operator, value = self._parse_condition(i)
                            column = self.column_by_name(column_name)
                            index_rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
                        if count == 0:
                            and_rows = index_rows
                        else:
                            and_rows = set(and_rows).intersection(index_rows)
                        count += 1
                    indexes_to_del += and_rows
                if _or: # Same as above but the produced rows go to indexes_to_del
                    for i in _or:
                        if ' and ' not in i or i.count(' and ') == i.count('between '):
                            if 'not between ' in i:
                                column_name, values = self._parse_condition(i.replace('not ', ''))
                                column = self.column_by_name(column_name)
                                index_rows = [ind for ind, x in enumerate(column) if
                                              not get_op_between(x, values[0], values[1])]
                            elif 'between ' in i:
                                column_name, values = self._parse_condition(i)
                                column = self.column_by_name(column_name)
                                index_rows = [ind for ind, x in enumerate(column) if
                                              get_op_between(x, values[0], values[1])]
                            elif 'not ' in i:
                                column_name, operator, value = self._parse_condition(i.replace('not ', ''))
                                column = self.column_by_name(column_name)
                                index_rows = [ind for ind, x in enumerate(column) if not get_op(operator, x, value)]
                            else:
                                column_name, operator, value = self._parse_condition(i)
                                column = self.column_by_name(column_name)
                                index_rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
                            indexes_to_del += index_rows
            indexes_to_del += list(set(indexes_to_del)) # Removing duplicates

        # we pop from highest to lowest index in order to avoid removing the wrong item
        # since we dont delete, we dont have to to pop in that order, but since delete is used
        # to delete from meta tables too, we still implement it.

        for index in sorted(indexes_to_del, reverse=True):
            if self._name[:4] != 'meta':
                # if the table is not a metatable, replace the row with a row of nones
                self.data[index] = [None for _ in range(len(self.column_names))]
            else:
                self.data.pop(index)

        # self._update()
        # we have to return the deleted indexes, since they will be appended to the insert_stack
        return indexes_to_del

    def _select_where(self, return_columns, condition=None, distinct=False, order_by=None, desc=True, limit=None):
        '''
        Select and return a table containing specified columns and rows where condition is met.

        Args:
            return_columns: list. The columns to be returned.
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.
                
                Operatores supported: (<,<=,==,>=,>)
            distinct: boolean. If True, the resulting table will contain only unique rows (False by default).
            order_by: string. A column name that signals that the resulting table should be ordered based on it (no order if None).
            desc: boolean. If True, order_by will return results in descending order (False by default).
            limit: int. An integer that defines the number of rows that will be returned (all rows if None).
        '''
        _or = [] # Same as above
        _and = []
        if condition is not None:
            if ' or ' in condition:
                _or = condition.split(' or ')
                for i in _or:
                    if ' and ' in i:
                        _and.append(i.split(' and '))
            elif ' and ' in condition:
                _and = [condition.split(' and ')]
            if ' and ' in condition:
                for i in range(len(_and)):
                    end = len(_and[i])
                    j = 0
                    while j < end:
                        if 'between ' in _and[i][j]:
                            _and[i][j:j + 2] = [' and '.join(_and[i][j:j + 2])]
                            end -= 1
                        j += 1
        # if * return all columns, else find the column indexes for the columns specified
        if return_columns == '*':
            return_cols = [i for i in range(len(self.column_names))]
        else:
            return_cols = [self.column_names.index(col.strip()) for col in return_columns.split(',')]

        # if condition is None, return all rows
        # if not, return the rows with values where condition is met for value
        if condition is not None and condition.count(' and ') == condition.count('between ') and not _or: # Same as above
            if 'not between ' in condition:
                column_name, values = self._parse_condition(condition.replace('not ', ''))
                column = self.column_by_name(column_name)
                rows = [ind for ind, x in enumerate(column) if not get_op_between(x, values[0], values[1])]
            elif 'between ' in condition:
                column_name, values = self._parse_condition(condition)
                column = self.column_by_name(column_name)
                rows = [ind for ind, x in enumerate(column) if get_op_between(x, values[0], values[1])]
            elif 'not ' in condition:
                column_name, operator, value = self._parse_condition(condition.replace('not ', ''))
                column = self.column_by_name(column_name)
                rows = [ind for ind, x in enumerate(column) if not get_op(operator, x, value)]
            else:
                column_name, operator, value = self._parse_condition(condition)
                column = self.column_by_name(column_name)
                rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
        elif _and or _or:
            rows = []
            if _or and not _and:
                for i in _or:
                    if 'not ' in i:
                        column_name, operator, value = self._parse_condition(i.replace('not ', ''))
                        column = self.column_by_name(column_name)
                        index_rows = [ind for ind, x in enumerate(column) if not get_op(operator, x, value)]
                    else:
                        column_name, operator, value = self._parse_condition(i)
                        column = self.column_by_name(column_name)
                        index_rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
                    rows += index_rows
                rows = list(set(rows))
            elif _and:
                rows = []
                for j in _and:
                    and_rows = []
                    count = 0
                    for i in j:
                        if 'not between ' in i:
                            column_name, values = self._parse_condition(i.replace('not ', ''))
                            column = self.column_by_name(column_name)
                            index_rows = [ind for ind, x in enumerate(column) if
                                          not get_op_between(x, values[0], values[1])]
                        elif 'between ' in i:
                            column_name, values = self._parse_condition(i)
                            column = self.column_by_name(column_name)
                            index_rows = [ind for ind, x in enumerate(column) if
                                          get_op_between(x, values[0], values[1])]
                        elif 'not ' in i:
                            column_name, operator, value = self._parse_condition(i.replace('not ', ''))
                            column = self.column_by_name(column_name)
                            index_rows = [ind for ind, x in enumerate(column) if not get_op(operator, x, value)]
                        else:
                            column_name, operator, value = self._parse_condition(i)
                            column = self.column_by_name(column_name)
                            index_rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
                        if count == 0:
                            and_rows = index_rows
                        else:
                            and_rows = set(and_rows).intersection(index_rows)
                        count += 1
                    rows += and_rows
                if _or:
                    for i in _or:
                        if ' and ' not in i or i.count(' and ') == i.count('between '):
                            if 'not between ' in i:
                                column_name, values = self._parse_condition(i.replace('not ', ''))
                                column = self.column_by_name(column_name)
                                index_rows = [ind for ind, x in enumerate(column) if
                                              not get_op_between(x, values[0], values[1])]
                            elif 'between ' in i:
                                column_name, values = self._parse_condition(i)
                                column = self.column_by_name(column_name)
                                index_rows = [ind for ind, x in enumerate(column) if
                                              get_op_between(x, values[0], values[1])]
                            elif 'not ' in i:
                                column_name, operator, value = self._parse_condition(i.replace('not ', ''))
                                column = self.column_by_name(column_name)
                                index_rows = [ind for ind, x in enumerate(column) if not get_op(operator, x, value)]
                            else:
                                column_name, operator, value = self._parse_condition(i)
                                column = self.column_by_name(column_name)
                                index_rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
                            rows += index_rows
            rows = list(set(rows))
        else:
            rows = [i for i in range(len(self.data))]

        # copy the old dict, but only the rows and columns of data with index in rows/columns (the indexes that we want returned)
        dict = {(key): ([[self.data[i][j] for j in return_cols] for i in rows] if key == "data" else value) for
                key, value in self.__dict__.items()}

        # we need to set the new column names/types and no of columns, since we might
        # only return some columns
        dict['column_names'] = [self.column_names[i] for i in return_cols]
        dict['column_types'] = [self.column_types[i] for i in return_cols]

        s_table = Table(load=dict)

        s_table.data = list(set(map(lambda x: tuple(x), s_table.data))) if distinct else s_table.data

        if order_by:
            s_table.order_by(order_by, desc)

        # if isinstance(limit, str):
        #     try:
        #         k = int(limit)
        #     except ValueError:
        #         raise Exception("The value following 'top' in the query should be a number.")

        #     # Remove from the table's data all the None-filled rows, as they are not shown by default
        #     # Then, show the first k rows 
        #     s_table.data.remove(len(s_table.column_names) * [None])
        #     s_table.data = s_table.data[:k]
        if isinstance(limit, str):
            s_table.data = [row for row in s_table.data if any(row)][:int(limit)]

        return s_table

    def _select_where_with_btree(self, return_columns, bt, condition, index_column, distinct=False, order_by=None, desc=True, limit=None):
        _or = [] # Same as above
        _and = []
        if condition is not None:
            if ' or ' in condition:
                _or = condition.split(' or ')
                for i in _or:
                    if ' and ' in i:
                        _and.append(i.split(' and '))
            elif ' and ' in condition:
                _and = [condition.split(' and ')]
            if ' and ' in condition:
                for i in range(len(_and)):
                    end = len(_and[i])
                    j = 0
                    while j < end:
                        if 'between ' in _and[i][j]:
                            _and[i][j:j + 2] = [' and '.join(_and[i][j:j + 2])]
                            end -= 1
                        j += 1

        # if * return all columns, else find the column indexes for the columns specified
        if return_columns == '*':
            return_cols = [i for i in range(len(self.column_names))]
        else:
            return_cols = [self.column_names.index(colname) for colname in return_columns]
        # Same as above but bt.find is used when on index column
        if condition is not None and condition.count(' and ') == condition.count('between ') and not _or:
            if 'not between ' in condition:
                column_name, values = self._parse_condition(condition.replace('not ', ''))
                column = self.column_by_name(column_name)
                if column_name == index_column: # If column name is the index column do btree searce
                    if values[0] <= values[1]: # Check if left value is smaller than the right value
                        rows = bt.find('<', values[0]) # Find values greater than the smallest value in values list
                        rows = list(set(rows).intersection(bt.find('>', values[1]))) # Add the values smaller than the biggest value in values list
                    else:
                        rows = bt.find('<', values[1]) # Find values greater than the smallest value in values list
                        rows = list(set(rows).intersection(bt.find('>', values[0]))) # Add the values smaller than the biggest value in values list
                else:
                    rows = [ind for ind, x in enumerate(column) if not get_op_between(x, values[0], values[1])]
            elif 'between ' in condition:
                column_name, values = self._parse_condition(condition)
                column = self.column_by_name(column_name)
                if column_name == index_column:
                    if values[0] <= values[1]:  # Check if left value is smaller than the right value
                        rows = bt.find('>=', values[0]) # Find values smaller than or equal to the biggest value in values list
                        rows = list(set(rows).intersection(bt.find('<=', values[1])))
                    else:
                        rows = bt.find('>=', values[1])
                        rows = list(set(rows).intersection(bt.find('<=', values[0])))
                else:
                    rows = [ind for ind, x in enumerate(column) if get_op_between(x, values[0], values[1])]
            elif 'not ' in condition:
                column_name, operator, value = self._parse_condition(condition.replace('not ', ''))
                column = self.column_by_name(column_name)
                if column_name == index_column:
                    if operator == '>':
                        rows = bt.find('<=', value)
                    elif operator == '<':
                        rows = bt.find('>=', value)
                    elif operator == '>=':
                        rows = bt.find('<', value)
                    elif operator == '<=':
                        rows = bt.find('>', value)
                    else:
                        rows = bt.find('<', value)
                        rows += bt.find('>', value)
                        rows = list(set(rows))
                else:
                    rows = [ind for ind, x in enumerate(column) if not get_op(operator, x, value)]
            else:
                column_name, operator, value = self._parse_condition(condition)
                column = self.column_by_name(column_name)
                if column_name == index_column:
                    rows = bt.find(operator, value)
                else:
                    rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
        elif _and or _or:
            rows = []
            if _or and not _and:
                for i in _or:
                    if 'not ' in i:
                        column_name, operator, value = self._parse_condition(i.replace('not ', ''))
                        column = self.column_by_name(column_name)
                        if column_name == index_column:
                            if operator == '>':
                                index_rows = bt.find('<=', value)
                            elif operator == '<':
                                index_rows = bt.find('>=', value)
                            elif operator == '>=':
                                index_rows = bt.find('<', value)
                            elif operator == '<=':
                                index_rows = bt.find('>', value)
                            else:
                                index_rows = bt.find('<', value)
                                index_rows += bt.find('>', value)
                                index_rows = list(set(index_rows))
                        else:
                            index_rows = [ind for ind, x in enumerate(column) if not get_op(operator, x, value)]
                    else:
                        column_name, operator, value = self._parse_condition(i)
                        column = self.column_by_name(column_name)
                        if column_name == index_column:
                            index_rows = bt.find(operator, value)
                        else:
                            index_rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
                            print(index_rows)
                    rows += index_rows
                rows = list(set(rows))
            elif _and:
                rows = []
                for j in _and:
                    and_rows = []
                    count = 0
                    for i in j:
                        if 'not between ' in i:
                            column_name, values = self._parse_condition(i.replace('not ', ''))
                            column = self.column_by_name(column_name)
                            if column_name == index_column:  # If column name is the index column do btree searce
                                if values[0] <= values[1]:  # Check if left value is smaller than the right value
                                    index_rows = bt.find('<', values[0])  # Find values greater than the smallest value in values list
                                    index_rows = list(set(index_rows).intersection(bt.find('>', values[1])))  # Add the values smaller than the biggest value in values list
                                else:
                                    index_rows = bt.find('<', values[1])  # Find values greater than the smallest value in values list
                                    index_rows = list(set(index_rows).intersection(bt.find('>', values[0])))  # Add the values smaller than the biggest value in values list
                            else:
                                index_rows = [ind for ind, x in enumerate(column) if not get_op_between(x, values[0], values[1])]
                        elif 'between ' in i:
                            column_name, values = self._parse_condition(i)
                            column = self.column_by_name(column_name)
                            if column_name == index_column:
                                if values[0] <= values[1]:  # Check if left value is smaller than the right value
                                    index_rows = bt.find('>=', values[0])
                                    index_rows = list(set(index_rows).intersection(bt.find('<=', values[1])))
                                else:
                                    index_rows = bt.find('>=', values[1])
                                    index_rows = list(set(index_rows).intersection(bt.find('<=', values[0])))
                            else:
                                index_rows = [ind for ind, x in enumerate(column) if get_op_between(x, values[0], values[1])]
                        elif 'not ' in i:
                            column_name, operator, value = self._parse_condition(i.replace('not ', ''))
                            column = self.column_by_name(column_name)
                            if column_name == index_column:
                                if operator == '>':
                                    index_rows = bt.find('<=', value)
                                elif operator == '<':
                                    index_rows = bt.find('>=', value)
                                elif operator == '>=':
                                    index_rows = bt.find('<', value)
                                elif operator == '<=':
                                    index_rows = bt.find('>', value)
                                else:
                                    index_rows = bt.find('<', value)
                                    index_rows += bt.find('>', value)
                                    index_rows = list(set(index_rows))
                            else:
                                index_rows = [ind for ind, x in enumerate(column) if not get_op(operator, x, value)]
                        else:
                            column_name, operator, value = self._parse_condition(i)
                            column = self.column_by_name(column_name)
                            if column_name == index_column:
                                index_rows = bt.find(operator, value)
                            else:
                                index_rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
                        if count == 0:
                            and_rows = index_rows
                        else:
                            and_rows = set(and_rows).intersection(index_rows)
                        count += 1
                    rows += and_rows
                if _or:
                    for i in _or:
                        if ' and ' not in i or i.count(' and ') == i.count('between '):
                            if 'not between ' in i:
                                column_name, values = self._parse_condition(i.replace('not ', ''))
                                column = self.column_by_name(column_name)
                                if column_name == index_column:  # If column name is the index column do btree searce
                                    if values[0] <= values[1]:  # Check if left value is smaller than the right value
                                        index_rows = bt.find('<', values[0])  # Find values greater than the smallest value in values list
                                        index_rows = list(set(index_rows).intersection(bt.find('>', values[1])))  # Add the values smaller than the biggest value in values list
                                    else:
                                        index_rows = bt.find('<', values[1])  # Find values greater than the smallest value in values list
                                        index_rows = list(set(index_rows).intersection(bt.find('>', values[0])))  # Add the values smaller than the biggest value in values list
                                else:
                                    index_rows = [ind for ind, x in enumerate(column) if not get_op_between(x, values[0], values[1])]
                            elif 'between ' in i:
                                column_name, values = self._parse_condition(i)
                                column = self.column_by_name(column_name)
                                if column_name == index_column:
                                    if values[0] <= values[1]:  # Check if left value is smaller than the right value
                                        index_rows = bt.find('>=', values[0])
                                        index_rows = list(set(index_rows).intersection(bt.find('<=', values[1])))
                                    else:
                                        index_rows = bt.find('>=', values[1])
                                        index_rows = list(set(index_rows).intersection(bt.find('<=', values[0])))
                                else:
                                    index_rows = [ind for ind, x in enumerate(column) if
                                                  get_op_between(x, values[0], values[1])]
                            elif 'not ' in i:
                                column_name, operator, value = self._parse_condition(i.replace('not ', ''))
                                column = self.column_by_name(column_name)
                                if column_name == index_column:
                                    if operator == '>':
                                        index_rows = bt.find('<=', value)
                                    elif operator == '<':
                                        index_rows = bt.find('>=', value)
                                    elif operator == '>=':
                                        index_rows = bt.find('<', value)
                                    elif operator == '<=':
                                        index_rows = bt.find('>', value)
                                    else:
                                        index_rows = bt.find('<', value)
                                        index_rows += bt.find('>', value)
                                        index_rows = list(set(index_rows))
                                else:
                                    index_rows = [ind for ind, x in enumerate(column) if not get_op(operator, x, value)]
                            else:
                                column_name, operator, value = self._parse_condition(i)
                                column = self.column_by_name(column_name)
                                if column_name == index_column:
                                    index_rows = bt.find(operator, value)
                                else:
                                    index_rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
                            rows += index_rows
            rows = list(set(rows))
        else:
            rows = [i for i in range(len(self.data))]

        try:
            k = int(limit)
        except TypeError:
            k = None
        # same as simple select from now on
        rows = rows[:k]
        # TODO: this needs to be dumbed down
        dict = {(key): ([[self.data[i][j] for j in return_cols] for i in rows] if key == "data" else value) for
                key, value in self.__dict__.items()}

        dict['column_names'] = [self.column_names[i] for i in return_cols]
        dict['column_types'] = [self.column_types[i] for i in return_cols]

        s_table = Table(load=dict)

        s_table.data = list(set(map(lambda x: tuple(x), s_table.data))) if distinct else s_table.data

        if order_by:
            s_table.order_by(order_by, desc)

        if isinstance(limit, str):
            s_table.data = [row for row in s_table.data if row is not None][:int(limit)]

        return s_table

    """
    Returns a table of the rows where the condition is true when the selected table has a hash function
    """
    def _select_where_with_hash(self, return_columns, hash, condition, index_column, distinct=False, order_by=None, desc=True, limit=None):
        _or = [] # Same as above
        _and = []
        if condition is not None:
            if ' or ' in condition:
                _or = condition.split(' or ')
                for i in _or:
                    if ' and ' in i:
                        _and.append(i.split(' and '))
            elif ' and ' in condition:
                _and = [condition.split(' and ')]
            if ' and ' in condition:
                for i in range(len(_and)):
                    end = len(_and[i])
                    j = 0
                    while j < end:
                        if 'between ' in _and[i][j]:
                            _and[i][j:j + 2] = [' and '.join(_and[i][j:j + 2])]
                            end -= 1
                        j += 1

        if return_columns == '*':
            return_cols = [i for i in range(len(self.column_names))]
        else:
            return_cols = [self.column_names.index(colname) for colname in return_columns]

        # Same as above but on '=' operations hash find is used
        rows = []
        if condition is not None and condition.count(' and ') == condition.count('between ') and not _or:
            if 'not between ' in condition:
                column_name, values = self._parse_condition(condition.replace('not ', ''))
                column = self.column_by_name(column_name)
                rows = [ind for ind, x in enumerate(column) if not get_op_between(x, values[0], values[1])]
            elif 'between ' in condition:
                column_name, values = self._parse_condition(condition)
                column = self.column_by_name(column_name)
                rows = [ind for ind, x in enumerate(column) if get_op_between(x, values[0], values[1])]
            elif 'not ' in condition:
                column_name, operator, value = self._parse_condition(condition.replace('not ', ''))
                column = self.column_by_name(column_name)
                rows = [ind for ind, x in enumerate(column) if not get_op(operator, x, value)]
            else:
                column_name, operator, value = self._parse_condition(condition)
                column = self.column_by_name(column_name)
                if operator == '=' and column_name == index_column: # Do hash find if operation is = and the condition column is the index column
                    sum = 0 # Hash find sums the characters in each word using the ord() function and by using a hash function it finds the right row
                    value = str(value) # Make the value a string so that if it wasn't a string it could still produce a sum using the ord() function
                    for key in value:
                        sum += ord(key)
                    index = sum % hash['Rows Length'] # Hash function takes the modulo sum with the amount rows present in the table by searching in the hash dictionary the value 'Rows Length'
                    for keys in hash[index]: # if the value exists in the hash with the value of the index add the row to rows
                        rows.append(keys[0])
                else: # If '=' operation is not used use the ordinary way to find rows
                    rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
        elif _and or _or:
            rows = []
            if _or and not _and:
                for i in _or:
                    if 'not ' in i:
                        column_name, operator, value = self._parse_condition(i.replace('not ', ''))
                        column = self.column_by_name(column_name)
                        index_rows = [ind for ind, x in enumerate(column) if not get_op(operator, x, value)]
                    else:
                        column_name, operator, value = self._parse_condition(i)
                        column = self.column_by_name(column_name)
                        if column_name == index_column:
                            if operator == '=':
                                sum = 0
                                value = str(value)
                                for key in value:
                                    sum += ord(key)
                                index = sum % hash['Rows Length']
                                for keys in hash[index]:
                                    index_rows = [keys[0]]
                            else:
                                index_rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
                        else:
                            index_rows = [ind for ind, x in enumerate(column) if not get_op(operator, x, value)]
                    rows += index_rows
                rows = list(set(rows))
            elif _and:
                rows = []
                for j in _and:
                    and_rows = []
                    count = 0
                    for i in j:
                        if 'not between ' in i:
                            column_name, values = self._parse_condition(i.replace('not ', ''))
                            column = self.column_by_name(column_name)
                            index_rows = [ind for ind, x in enumerate(column) if
                                          not get_op_between(x, values[0], values[1])]
                        elif 'between ' in i:
                            column_name, values = self._parse_condition(i)
                            column = self.column_by_name(column_name)
                            index_rows = [ind for ind, x in enumerate(column) if
                                          get_op_between(x, values[0], values[1])]
                        elif 'not ' in i:
                            column_name, operator, value = self._parse_condition(i.replace('not ', ''))
                            column = self.column_by_name(column_name)
                            index_rows = [ind for ind, x in enumerate(column) if not get_op(operator, x, value)]
                        else:
                            column_name, operator, value = self._parse_condition(i)
                            column = self.column_by_name(column_name)
                            if column_name == index_column:
                                if operator == '=':
                                    sum = 0
                                    value = str(value)
                                    for key in value:
                                        sum += ord(key)
                                    index = sum % hash['Rows Length']
                                    for keys in hash[index]:
                                        index_rows = [keys[0]]
                                else:
                                    index_rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
                            else:
                                index_rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
                        if count == 0:
                            and_rows = index_rows
                        else:
                            and_rows = set(and_rows).intersection(index_rows)
                        count += 1
                    rows += and_rows
                if _or:
                    for i in _or:
                        if ' and ' not in i or i.count(' and ') == i.count('between '):
                            if 'not between ' in i:
                                column_name, values = self._parse_condition(i.replace('not ', ''))
                                column = self.column_by_name(column_name)
                                index_rows = [ind for ind, x in enumerate(column) if
                                              not get_op_between(x, values[0], values[1])]
                            elif 'between ' in i:
                                column_name, values = self._parse_condition(i)
                                column = self.column_by_name(column_name)
                                index_rows = [ind for ind, x in enumerate(column) if
                                              get_op_between(x, values[0], values[1])]
                            elif 'not ' in i:
                                column_name, operator, value = self._parse_condition(i.replace('not ', ''))
                                column = self.column_by_name(column_name)
                                index_rows = [ind for ind, x in enumerate(column) if not get_op(operator, x, value)]
                            else:
                                column_name, operator, value = self._parse_condition(i)
                                column = self.column_by_name(column_name)
                                if column_name == index_column:
                                    if operator == '=':
                                        sum = 0
                                        value = str(value)
                                        for key in value:
                                            sum += ord(key)
                                        index = sum % hash['Rows Length']
                                        for keys in hash[index]:
                                            index_rows = [keys[0]]
                                    else:
                                        index_rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
                                else:
                                    index_rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
                            rows += index_rows
            rows = list(set(rows))
        else:
            rows = [i for i in range(len(self.data))]

        try:
            k = int(limit)
        except TypeError:
            k = None
        # same as simple select from now on
        rows = rows[:k]
        # TODO: this needs to be dumbed down
        dict = {(key): ([[self.data[i][j] for j in return_cols] for i in rows] if key == "data" else value) for
                key, value in self.__dict__.items()}

        dict['column_names'] = [self.column_names[i] for i in return_cols]
        dict['column_types'] = [self.column_types[i] for i in return_cols]

        s_table = Table(load=dict)

        s_table.data = list(set(map(lambda x: tuple(x), s_table.data))) if distinct else s_table.data

        if order_by:
            s_table.order_by(order_by, desc)

        if isinstance(limit, str):
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

    def _general_join_processing(self, table_right: Table, condition, join_type):
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

        if (operator != '=' and join_type in ['left', 'right', 'full']):
            class CustomFailException(Exception):
                pass

            raise CustomFailException('Outer Joins can only be used if the condition operator is "=".\n')

        try:
            column_index_left = self.column_names.index(column_name_left)
        except:
            raise Exception(
                f'Column "{column_name_left}" dont exist in left table. Valid columns: {self.column_names}.')

        try:
            column_index_right = table_right.column_names.index(column_name_right)
        except:
            raise Exception(
                f'Column "{column_name_right}" dont exist in right table. Valid columns: {table_right.column_names}.')

        # get the column names of both tables with the table name in front
        # ex. for left -> name becomes left_table_name_name etc
        left_names = [f'{self._name}.{colname}' if self._name != '' else colname for colname in self.column_names]
        right_names = [f'{table_right._name}.{colname}' if table_right._name != '' else colname for colname in
                       table_right.column_names]

        # define the new tables name, its column names and types
        join_table_name = ''
        join_table_colnames = left_names + right_names
        join_table_coltypes = self.column_types + table_right.column_types
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types=join_table_coltypes)

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
        join_table, column_index_left, column_index_right, operator = self._general_join_processing(table_right,
                                                                                                    condition, 'inner')

        # count the number of operations (<,> etc)
        no_of_ops = 0
        # this code is dumb on purpose... it needs to illustrate the underline technique
        # for each value in left column and right column, if condition, append the corresponding row to the new table
        for row_left in self.data:
            left_value = row_left[column_index_left]
            for row_right in table_right.data:
                right_value = row_right[column_index_right]
                if (left_value is None and right_value is None):
                    continue
                no_of_ops += 1
                if get_op(operator, left_value, right_value):  # EQ_OP
                    join_table._insert(row_left + row_right)

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
        join_table, column_index_left, column_index_right, operator = self._general_join_processing(table_right,
                                                                                                    condition, 'left')

        right_column = table_right.column_by_name(table_right.column_names[column_index_right])
        right_table_row_length = len(table_right.column_names)

        for row_left in self.data:
            left_value = row_left[column_index_left]
            if left_value is None:
                continue
            elif left_value not in right_column:
                join_table._insert(row_left + right_table_row_length * ["NULL"])
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
        join_table, column_index_left, column_index_right, operator = self._general_join_processing(table_right,
                                                                                                    condition, 'right')

        left_column = self.column_by_name(self.column_names[column_index_left])
        left_table_row_length = len(self.column_names)

        for row_right in table_right.data:
            right_value = row_right[column_index_right]
            if right_value is None:
                continue
            elif right_value not in left_column:
                join_table._insert(left_table_row_length * ["NULL"] + row_right)
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
        join_table, column_index_left, column_index_right, operator = self._general_join_processing(table_right,
                                                                                                    condition, 'full')

        right_column = table_right.column_by_name(table_right.column_names[column_index_right])
        left_column = self.column_by_name(self.column_names[column_index_left])

        right_table_row_length = len(table_right.column_names)
        left_table_row_length = len(self.column_names)

        for row_left in self.data:
            left_value = row_left[column_index_left]
            if left_value is None:
                continue
            if left_value not in right_column:
                join_table._insert(row_left + right_table_row_length * ["NULL"])
            else:
                for row_right in table_right.data:
                    right_value = row_right[column_index_right]
                    if left_value == right_value:
                        join_table._insert(row_left + row_right)

        for row_right in table_right.data:
            right_value = row_right[column_index_right]

            if right_value is None:
                continue
            elif right_value not in left_column:
                join_table._insert(left_table_row_length * ["NULL"] + row_right)

        return join_table

    def show(self, no_of_rows=None, is_locked=False):
        '''
        Print the table in a nice readable format.

        Args:
            no_of_rows: int. Number of rows.
            is_locked: boolean. Whether it is locked (False by default).
        '''
        output = ""
        # if the table is locked, add locked keyword to title
        if is_locked:
            output += f"\n## {self._name} (locked) ##\n"
        else:
            output += f"\n## {self._name} ##\n"

        # headers -> "column name (column type)"
        headers = [f'{col} ({tp.__name__})' for col, tp in zip(self.column_names, self.column_types)]
        if self.pk_idx is not None:
            # table has a primary key, add PK next to the appropriate column
            headers[self.pk_idx] = headers[self.pk_idx] + ' #PK#'
        # detect the rows that are no tfull of nones (these rows have been deleted)
        # if we dont skip these rows, the returning table has empty rows at the deleted positions
        non_none_rows = [row for row in self.data if any(row)]
        # print using tabulate
        print(tabulate(non_none_rows[:no_of_rows], headers=headers) + '\n')

    def _parse_condition(self, condition, join=False):
        '''
        Parse the single string condition and return the value of the column and the operator.

        Args:
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.
                
                Operatores supported: (<,<=,==,>=,>)
            join: boolean. Whether to join or not (False by default).
        '''
        # if both_columns (used by the join function) return the names of the names of the columns (left first)
        if join:
            return split_condition(condition)

        if 'between ' in condition: # If the between operator is used
            condition_column, values = split_condition_between(condition) # Get the condition_column and the right and left values around 'and'
            if condition_column not in self.column_names: # Raise error if the column was not found
                raise ValueError(f'Condition is not valid (cant find column name)')
            coltype = self.column_types[self.column_names.index(condition_column)] # Get the type of the condition_column
            values[0] = coltype(values[0]) # Cast the left value
            values[1] = coltype(values[1]) # Cast the right value
            return condition_column, values # Return the condition_column and the values
        else:
            # cast the value with the specified column's type and return the column name, the operator and the casted value
            left, op, right = split_condition(condition)

            if left not in self.column_names:
                raise ValueError(f'Condition is not valid (cant find column name)')
            coltype = self.column_types[self.column_names.index(left)]

            return left, op, coltype(right)

    def _load_from_file(self, filename):
        '''
        Load table from a pkl file (not used currently).

        Args:
            filename: string. Name of pkl file.
        '''
        f = open(filename, 'rb')
        tmp_dict = pickle.load(f)
        f.close()

        self.__dict__.update(tmp_dict.__dict__)
