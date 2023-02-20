from __future__ import annotations
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
        - unique (list of strings (column names) that are unique)

    OR

        - by assigning a value to the variable called load. This value can be:
            - a path to a Table file saved using the save function
            - a dictionary that includes the appropriate info (all the attributes in __init__)

    '''
    def __init__(self, name=None, column_names=None, column_types=None, primary_key=None, unique_columns=None, load=None):

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

            self.column_types = [eval(ct) if not isinstance(ct, type) else ct for ct in column_types]
            self.data = [] # data is a list of lists, a list of rows that is.

            # if primary key is set, keep its index as an attribute
            if primary_key is not None:
                self.pk_idx = self.column_names.index(primary_key)
            else:
                self.pk_idx = None

            self.pk = primary_key
            
            # if unique columns are set, keep their names and indexes as attributes.
            self.unique_columns = None
            if unique_columns is not None:
                self.unique_columns = unique_columns
                self.unique_columns_idx = [self.column_names.index(col) for col in self.column_names if col in self.unique_columns]
                
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
            <> column_name: string. The column that will be casted.
            <> cast_type: type. Cast type (do not encapsulate in quotes).
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
            <> row: list. A list of values to be inserted (will be casted to a predifined type automatically).
            <> insert_stack: list. The insert stack (empty by default).
        '''
        if len(row)!=len(self.column_names):
            raise ValueError(f'ERROR -> Cannot insert {len(row)} values. Only {len(self.column_names)} columns exist')

        for i in range(len(row)):
            # for each value, cast and replace it in row.
            try:
                row[i] = self.column_types[i](row[i])
            except ValueError:
                if row[i] != 'NULL':
                    raise ValueError(f'ERROR -> Value {row[i]} of type {type(row[i])} is not of type {self.column_types[i]}.')
            except TypeError as exc:
                if row[i] != None:
                    print(exc)

            # if value is to be appended to the primary_key column, check that it doesn't already exist (no duplicate primary keys).
            if i==self.pk_idx and row[i] in self.column_by_name(self.pk):
                raise ValueError(f'ERROR -> Value "{row[i]}" already exists in primary key column.')
            elif i==self.pk_idx and (row[i] is None or row[i] == ''):
                raise ValueError(f'ERROR -> The value of the primary key cannot be None.')

            # if value already exists in a unique column, raise an error (no duplicate values in unique columns).
            if self.unique_columns is not None and i in self.unique_columns_idx and row[i] in self.column_by_name(self.column_names[i]):
                raise ValueError(f'## ERROR -> Value "{row[i]}" already exists in unique column "{self.column_names[i]}".')
            
        # if insert_stack is not empty, append to its last index
        if insert_stack != []:
            self.data[insert_stack[-1]] = row
        else: # else append to the end
            self.data.append(row)
        # self._update()

    def _update_rows(self, set_value, set_column, condition):
        '''
        Update where Condition is met.

        Args:
            <> set_value: string. The provided set value.
            <> set_column: string. The column to be altered.
            <> condition: string or dict (the condition is the returned dic['where'] from interpret method).
                Operatores supported: (<,<=,=,>=,>)
        '''

        # get the set column index
        set_column_idx = self.column_names.index(set_column)
        # set_columns_indx = [self.column_names.index(set_column_name) for set_column_name in set_column_names]
        
        if condition is not None:
            indexes_to_upd = self.find_rows_by_condition(condition) # get the indexes of the rows to be updated
        else: # if condition is None, update all rows
            indexes_to_upd = [i for i in range(len(self.data))] # get all indexes
            
        for idx in indexes_to_upd:
            self.data[idx][set_column_idx] = set_value

        # self._update()
                # print(f"Updated {len(indexes_to_del)} rows")

    def _delete_where(self, condition):
        '''
        Deletes rows where condition is met.

        Important: delete replaces the rows to be deleted with rows filled with Nones.
        These rows are then appended to the insert_stack.

        Args:
            <> condition: string or dict (the condition is the returned dic['where'] from interpret method).
                Operatores supported: (<,<=,=,>=,>)
        '''
        if condition is not None:
            indexes_to_del = self.find_rows_by_condition(condition) # get the indexes of the rows to be deleted
        else: # if condition is None, delete all rows
            indexes_to_del = [i for i in range(len(self.data))] # get all indexes
                
        # we pop from highest to lowest index in order to avoid removing the wrong item
        # since we dont delete, we dont have to pop in that order, but since delete is used
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

    def _select_where(self, return_columns, condition=None, distinct=False, order_by=None, desc=True, limit=None, btree_dic=None, hash_dic=None):
        '''
        Select and return a table containing specified columns and rows where condition is met.

        Args:
            <> return_columns: list. The columns to be returned.
            <> condition: string or dict (the condition is the returned dic['where'] from interpret method).
                Operatores supported: (<,<=,=,>=,>)
            <> distinct: boolean. If True, the resulting table will contain only unique rows (False by default).
            <> order_by: string. A column name that signals that the resulting table should be ordered based on it (no order if None).
            <> desc: boolean. If True, order_by will return results in descending order (False by default).
            <> limit: int. An integer that defines the number of rows that will be returned (all rows if None).
            <> btree_dic: dict. A dictionary containing the btree indexes of the table. If None, no btree indexes are used.
            <> hash_dic: dict. A dictionary containing the hash indexes of the table. If None, no hash indexes are used.
        '''

        # if * return all columns, else find the column indexes for the columns specified
        if return_columns == '*':
            return_cols = [i for i in range(len(self.column_names))]
        else:
            return_cols = [self.column_names.index(col.strip()) for col in return_columns.split(',')]

        # if condition is None, return all rows
        # if not, return the rows with values where condition is met for value
        if condition is not None:
            rows = self.find_rows_by_condition(condition, btree_dic, hash_dic) # get the rows where condition is met
        else:
            rows = [i for i in range(len(self.data))]

        # copy the old dict, but only the rows and columns of data with index in rows/columns (the indexes that we want returned)
        dic = {(key):([[self.data[i][j] for j in return_cols] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}

        # we need to set the new column names/types and no of columns, since we might
        # only return some columns
        dic['column_names'] = [self.column_names[i] for i in return_cols]
        dic['column_types'] = [self.column_types[i] for i in return_cols]

        s_table = Table(load=dic)

        s_table.data = list(set(map(lambda x: tuple(x), s_table.data))) if distinct else s_table.data # remove duplicates

        if order_by:
            s_table.order_by(order_by, desc)

        '''if isinstance(limit, str):
            try:
                k = int(limit)
            except ValueError:
                raise Exception("The value following 'top' in the query should be a number.")
            
            # Remove from the table's data all the None-filled rows, as they are not shown by default
            # Then, show the first k rows 
            s_table.data.remove(len(s_table.column_names) * [None])
            s_table.data = s_table.data[:k]'''
        if isinstance(limit,str):
            s_table.data = [row for row in s_table.data if any(row)][:int(limit)]

        return s_table

    def order_by(self, column_name, desc=True):
        '''
        Order table based on column.

        Args:
            <> column_name: string. Name of column.
            <> desc: boolean. If True, order_by will return results in descending order (False by default).
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
            <> condition: string. A condition using the following format:
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
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types=join_table_coltypes)

        return join_table, column_index_left, column_index_right, operator

    def _inner_join(self, table_right: Table, condition):
        '''
        Join table (left) with a supplied table (right) where condition is met.

        Args:
            <> condition: string. A condition using the following format:
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
            <> condition: string. A condition using the following format:
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
            <> condition: string. A condition using the following format:
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
            <> condition: string. A condition using the following format:
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

            if right_value is None:
                continue
            elif right_value not in left_column:
                join_table._insert(left_table_row_length*["NULL"] + row_right)

        return join_table

    def show(self, no_of_rows=None, is_locked=False):
        '''
        Print the table in a nice readable format.

        Args:
            <> no_of_rows: int. Number of rows.
            <> is_locked: boolean. Whether it is locked (False by default).
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
            headers[self.pk_idx] = headers[self.pk_idx]+' #PK#'
            
        if self.unique_columns is not None:
            # table has unique columns, add UNIQUE next to the appropriate columns
            for idx in self.unique_columns_idx:
                headers[idx] = headers[idx]+' #UNIQUE#'
                
        # detect the rows that are no tfull of nones (these rows have been deleted)
        # if we dont skip these rows, the returning table has empty rows at the deleted positions
        non_none_rows = [row for row in self.data if any(row)]
        # print using tabulate
        print(tabulate(non_none_rows[:no_of_rows], headers=headers)+'\n')

    def _parse_condition(self, condition, join=False):
        '''
        Parse the single string condition and return the value of the column and the operator.

        Args:
            <> condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.
                
                Operatores supported: (<,<=,==,>=,>)
            <> join: boolean. Whether to join or not (False by default).
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
        Load table from a pkl file (not used currently).

        Args:
            <> filename: string. Name of pkl file.
        '''
        f = open(filename, 'rb')
        tmp_dict = pickle.load(f)
        f.close()

        self.__dict__.update(tmp_dict.__dict__)

    def find_rows_by_condition(self, condition, btree_dic=None, hash_dic=None):
        '''
        Traverse the dictionary and return the rows that satisfy the condition.
        Args:
            <> condition: string or dictionary (the condition is the returned dic['where'] from interpret method).
            <> btree_dic: dictionary. The dictionary of btree, the key is the column name and the value is the btree object.
            if None, we don't have btree.
            <> hash_dic: dictionary. The dictionary of hash table, the key is the column name and the value is the hash table object.
            if None, we don't have hash table.
        '''
        if isinstance(condition, str):
            column_name, operator, value = self._parse_condition(condition)
            
            if hash_dic is not None and column_name in hash_dic.keys() and operator == '=':
                '''
                If hash_dic is not None and the operator is '=', we use the hash table to find the row.
                '''
                row = hash_dic[column_name]._get(value)
                if row is not None:
                    return [int(r) for r in list(str(row))]
            
            if btree_dic is not None and column_name in btree_dic.keys():
                '''
                If btree_dic is not None and the column_name is in the btree_dic keys, we use the btree to find the rows.
                '''
                rows = btree_dic[column_name].find(operator, value)
                return rows
            ''' 
            If we don't have btree or hash table, we use the linear search to find the rows.
            '''
            column = self.column_by_name(column_name)
            rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
            return rows
        elif 'and' in condition:
            left = self.find_rows_by_condition(condition['and']['left'], btree_dic, hash_dic)
            right = self.find_rows_by_condition(condition['and']['right'], btree_dic, hash_dic)
            intersection = set(left).intersection(set(right)) # get the intersection of left and right
            return list(intersection)
        elif 'or' in condition:
            left = self.find_rows_by_condition(condition['or']['left'], btree_dic, hash_dic)
            right = self.find_rows_by_condition(condition['or']['right'], btree_dic, hash_dic)
            union = set(left).union(set(right)) # get the union of left and right
            return list(union)
        elif 'not' in condition:
            all_rows = [i for i in range(len(self.data))] # get all rows
            result = self.find_rows_by_condition(condition['not'], btree_dic, hash_dic)
            not_result = set(all_rows) - set(result) # get the difference of all rows and result
            return list(not_result)
        elif 'between' in condition:
            column_name = condition['column']
            condition['between']['and']['left'] = f"{column_name}>={condition['between']['and']['left']}" # build the condition for left side of between
            condition['between']['and']['right'] = f"{column_name}<={condition['between']['and']['right']}" # build the condition for right side of between
            return self.find_rows_by_condition(condition['between'], btree_dic, hash_dic)