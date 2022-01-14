from __future__ import annotations
from tabulate import tabulate
import pickle
import os
import re
from misc import get_op, split_condition
from statistics import mean # used to calculate the avg in group by
import copy

"""

ISSUES: 

    Using cprofile, we reached the conclusion that _uddate should be removed. Its very slow. This and constant file i/o is a problem.

    These problems are partially solved by a server based protocol.

    Handling columns should be discussed. Generating column when a function is called is an option. (use property decorator)

    Removing columns all together is another option. 

    Server should be implemented ASAP. No need to be great, needs to work tho.

    All queries should be run from file or REPL (psql like). Only way to interact with mdb should be the server. strip()

TODO:

    Simple REPL

"""


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
        if len(row)!=len(self.column_names):
            raise ValueError(f'ERROR -> Cannot insert {len(row)} values. Only {len(self.column_names)} columns exist')

        for i in range(len(row)):
            # for each value, cast and replace it in row.
            # try:
            row[i] = self.column_types[i](row[i])
            # except:
            #     raise ValueError(f'ERROR -> Value {row[i]} of type {type(row[i])} is not of type {self.column_types[i]}.')

            # if value is to be appended to the primary_key column, check that it doesnt alrady exist (no duplicate primary keys)
            if i==self.pk_idx and row[i] in self.column_by_name(self.pk):
                raise ValueError(f'## ERROR -> Value {row[i]} already exists in primary key column.')

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
            set_value: string. The provided set value.
            set_column: string. The column to be altered.
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.
                
                Operatores supported: (<,<=,==,>=,>)
        '''
        # parse the condition
        column_name, operator, value = self._parse_condition(condition)

        # get the condition and the set column
        column = self.column_by_name(column_name)
        set_column_idx = self.column_names.index(set_column)

        # set_columns_indx = [self.column_names.index(set_column_name) for set_column_name in set_column_names]

        # for each value in column, if condition, replace it with set_value
        for row_ind, column_value in enumerate(column):
            if get_op(operator, column_value, value):
                self.data[row_ind][set_column_idx] = set_value

        # self._update()
                # print(f"Updated {len(indexes_to_del)} rows")


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
        column_name, operator, value = self._parse_condition(condition)

        indexes_to_del = []

        column = self.column_by_name(column_name)
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

        # self._update()
        # we have to return the deleted indexes, since they will be appended to the insert_stack
        return indexes_to_del


    def _select_where(self, return_columns, condition=None, group_by=None, having=None, order_by=None, desc=True, top_k=None, distinct=False):
        '''
        Select and return a table containing specified columns and rows where condition is met.

        Args:
            return_columns: list. The columns to be returned.
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.
                
                Operatores supported: (<,<=,==,>=,>)
            order_by: string. A column name that signals that the resulting table should be ordered based on it (no order if None).
            desc: boolean. If True, order_by will return results in descending order (False by default).
            top_k: int. An integer that defines the number of rows that will be returned (all rows if None).
            distinct: boolean. If True, the resulting table will contain only unique rows (False by default).
        '''

        def _groupby_sum(column):
            # Check if column items are numeric
            if not any([ self.column_types[self.column_names.index(column)] is x for x in [int, float]  ]): raise ValueError(f"{column} is not numeric")
            
            for x, y in dict.items():
                if any(isinstance(i, list) for i in y):
                    dict2[x].append( sum( [ z[self.column_names.index(column)] for z in y ] ) )
                else:
                    dict2[x].append( y[self.column_names.index(column)] )

        def _groupby_avg(column):
            if not any([ self.column_types[self.column_names.index(column)] is x for x in [int, float]  ]): raise ValueError(f"{column} is not numeric")
            
            for x, y in dict.items():
                if any(isinstance(i, list) for i in y):
                    dict2[x].append( mean( list(filter( (None).__ne__,[ z[self.column_names.index(column)] for z in y ] )) ) )
                else:
                    dict2[x].append( y[self.column_names.index(column)] )
            
        def _groupby_min(column):
            for x, y in dict.items():
                if any(isinstance(i, list) for i in y):
                    dict2[x].append( min( list(filter( (None).__ne__,[ z[self.column_names.index(column)] for z in y ] ))) )
                else:
                    dict2[x].append( y[self.column_names.index(column)] )

        def _groupby_max(column):
            for x, y in dict.items():
                if any(isinstance(i, list) for i in y):
                    dict2[x].append( max( list(filter( (None).__ne__,[ z[self.column_names.index(column)] for z in y ] ))) )
                else:
                    dict2[x].append( y[self.column_names.index(column)] )

        def _groupby_count(column):
            for x, y in dict.items():
                dict2[x].append(len(y))

        agg_funcs = {'sum': _groupby_sum, 'avg':_groupby_avg, 'min':_groupby_min, 'max':_groupby_max, 'count':_groupby_count}

        def _groupby_agg_func_input(column):
            # If column is agg func
            try:
                if column.split(' (')[0] in agg_funcs.keys():
                    return re.findall("\(([^\)]+)\)", column)[0].strip()
                else:
                    return column
            except AttributeError:
                return column

        # if * return all columns, else find the column indexes for the columns specified
        if return_columns == '*':
            return_cols = [i for i in range(len(self.column_names))]
        else:
            return_cols = []
            for col in return_columns.split(','):
                # if column not an agg function
                if col.split(' (')[0] not in agg_funcs.keys():
                    return_cols.append(self.column_names.index(col.strip()))
                else:
                    if _groupby_agg_func_input(col) == "*":  
                        return_cols.append(col)
                    else:
                        # make sure that col that is in agg func exists
                        self.column_names.index(_groupby_agg_func_input(col))
                        return_cols.append(col)

        # if condition is None, return all rows
        # if not, return the rows with values where condition is met for value
        if condition is not None:
            #implementation of the in operator in where condition
            if "IN" in condition.split() or "in" in condition.split():
                #split the condition that contains the 'in' operator and remove the parentheses.
                #The 0th index points to the table name, The 1st is the 'in' operator
                #and the permitted values are listed between the '(' and ')' elements
                condition_list = condition.split()
                values = condition_list[condition_list.index("(")+1:condition_list.index(")")]
                values = [v.replace(",","") for v in values]
                condition_list.remove("(")
                condition_list.remove(")")

                #if the values are given separated only by commas e.g. 5,3,2,1 then the permitted values are the elements of the split string
                #else, we need to search every element with an index >= 2, split it at the commas and only keep the non-None values
                #this way, we can accept both (5, 3, 10, 4) and (5,3, 10,4)
                if(len(condition_list) == 3):
                    values = condition_list[2].split(",")
                else:
                    values = []
                    for v in condition_list[2:]:
                        vv = v.split(",")
                        for _ in vv:
                            if(_ != ''):
                                values.append(_)

                column_name = condition_list[0]
                column = self.column_by_name(column_name)
                rows = [ind for ind, x in enumerate(column) if (str(x) in values)]

            #implementation of the between operator in where condition
            elif "BETWEEN" in condition.split() or "between" in condition.split():
                condition_list = condition.split()
                _min = condition_list[2]
                _max = condition_list[4]
                column_name = condition_list[0]
                column = self.column_by_name(column_name)

                #The try block will run in the case of a string comparison 'between' operation
                try:
                    rows = [ind for ind, x in enumerate(column) if x >= _min and x <= _max]
                #The except block will run in the case of an integer comparison 'between' operation
                except:
                    rows = [ind for ind, x in enumerate(column) if (x >= int(_min) and x <= int(_max))]
            
            elif "LIKE" in condition.split() or "like" in condition.split():
                condition_list = condition.split()
                column_name = condition_list[0]
                column = self.column_by_name(column_name)

                sql_regex = ' '.join(condition_list[2:])

                # Convert SQL regex pattern to python regex pattern
                regex = re.compile(sql_regex.replace("'","").replace("%",".*").replace("_","."))

                # Add the rows that fully match the regex given
                rows = [ind for ind, x in enumerate(column) if regex.fullmatch(str(x))]

            else:
                column_name, operator, value = self._parse_condition(condition)
                column = self.column_by_name(column_name)
                rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
        else:
            rows = [i for i in range(len(self.data))]


        if group_by:
            
            # GROUP BY Checkers
            ## Check if any GROUP BY clause is aggregate function
            if any([x.split(' (')[0] in agg_funcs.keys() for x in group_by.split(',')]): raise ValueError("Aggregate functions are not allowed in GROUP BY")
            ## Check if selected columns (except of aggregate functions) are also clause of GROUP BY
            if not set(set([x for x in return_columns.split(',') if x.split(' (')[0] not in agg_funcs.keys()])).issubset( group_by.split(',') ): raise ValueError("A column you have selected, is not clause of group by.")

            # dict structure creation
            unique_values = set( [value[self.column_names.index(group_by)] for value in self.data] )
            dict = {x:[] for x in unique_values }
            dict2 = copy.deepcopy(dict)

            # Group data by a specific column
            for value in unique_values:
                for row in self.data:
                    if row[self.column_names.index(group_by)] == value:
                        dict[row[self.column_names.index(group_by)]].append(row)


            # Create final table
            for cur_sel_column in return_columns.split(','): # [x for x in return_columns.split(',') if x.split(' (')[0] in agg_funcs]:
                # Calculate aggregate functions if selected
                if cur_sel_column.split(' (')[0] in agg_funcs.keys():
                    agg_funcs.get( cur_sel_column.split(' (')[0] )( _groupby_agg_func_input(cur_sel_column) )

            
            dict = dict2 ; del dict2 # set the final table
            dict2 = {}

            for x in dict:
                dict[x].insert(return_columns.index(group_by), x)
            
            dict2['data'] = list(dict.values())
            dict = dict2 ; del dict2 # set the final table

            dict['column_names'] = [i for i in return_columns.split(',')]
            dict['_name'] = self._name

            dict['column_types'] = []
            for i in return_columns.split(','):
                if i.split(' (')[0] in agg_funcs.keys():
                    if i.split(' (')[0] in ['count','sum']:
                        dict['column_types'].append(type(1))
                    elif i.split(' (')[0] == 'avg':
                        dict['column_types'].append(type(1.1))
                    else:
                        dict['column_types'].append(self.column_types[self.column_names.index(_groupby_agg_func_input(i))])
                else:
                        dict['column_types'].append(self.column_types[self.column_names.index(_groupby_agg_func_input(i))])

            for key,value in self.__dict__.items():
                if key not in dict.keys():
                    dict[key] = value

        else:

            # top k rows
            # rows = rows[:int(top_k)] if isinstance(top_k,str) else rows
            # copy the old dict, but only the rows and columns of data with index in rows/columns (the indexes that we want returned)
            dict = {(key):([[self.data[i][j] for j in return_cols] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}

            
            # we need to set the new column names/types and no of columns, since we might
            # only return some columns
            dict['column_names'] = [self.column_names[i] for i in return_cols]
            dict['column_types'] = [self.column_types[i] for i in return_cols]

        

        s_table = Table(load=dict) 
        if order_by:
            s_table.order_by(order_by, desc)

        s_table.data = s_table.data[:int(top_k)] if isinstance(top_k,str) else s_table.data

        # If distinct is True, we remove duplicate rows from the resulting table
        s_table.data = list(set(map(lambda x: tuple(x), s_table.data))) if distinct else s_table.data

        return s_table


    def _select_where_with_btree(self, return_columns, bt, condition, group_by=None, having=None, order_by=None, desc=True, top_k=None, distinct=False):

        # if * return all columns, else find the column indexes for the columns specified
        if return_columns == '*':
            return_cols = [i for i in range(len(self.column_names))]
        else:
            return_cols = [self.column_names.index(colname) for colname in return_columns]


        column_name, operator, value = self._parse_condition(condition)

        # if the column in condition is not a primary key, abort the select
        if column_name != self.column_names[self.pk_idx]:
            print('Column is not PK. Aborting')

        # here we run the same select twice, sequentially and using the btree.
        # we then check the results match and compare performance (number of operation)
        column = self.column_by_name(column_name)

        # sequential
        rows1 = []
        opsseq = 0
        for ind, x in enumerate(column):
            opsseq+=1
            if get_op(operator, x, value):
                rows1.append(ind)

        # btree find
        rows = bt.find(operator, value)

        # same as simple select from now on
        rows = rows[:top_k]
        # TODO: this needs to be dumbed down
        dict = {(key):([[self.data[i][j] for j in return_cols] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}

        dict['column_names'] = [self.column_names[i] for i in return_cols]
        dict['column_types']   = [self.column_types[i] for i in return_cols]

        s_table = Table(load=dict) 
        if order_by:
            s_table.order_by(order_by, desc)

        s_table.data = s_table.data[:int(top_k)] if isinstance(top_k,str) else s_table.data

        # If distinct is True, we remove duplicate rows from the resulting table
        s_table.data = list(set(map(lambda x: tuple(x), s_table.data))) if distinct else s_table.data

        return s_table

    def order_by(self, column_name, desc=True):
        '''
        Order table based on column.

        Args:
            column_name: string. Name of column.
            desc: boolean. If True, order_by will return results in descending order (False by default).
        '''
        column = self.column_by_name(column_name)
        idx = sorted(range(len(column)), key=lambda k: column[k], reverse=desc)
        # print(idx)
        self.data = [self.data[i] for i in idx]
        # self._update()


    def _inner_join(self, table_right: Table, condition):
        '''
        Join table (left) with a supplied table (right) where condition is met.

        Args:
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.
                
                Operatores supported: (<,<=,==,>=,>)
        '''
        # get columns and operator
        column_name_left, operator, column_name_right = self._parse_condition(condition, join=True)
        # try to find both columns, if you fail raise error
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
            headers[self.pk_idx] = headers[self.pk_idx]+' #PK#'
        # detect the rows that are no tfull of nones (these rows have been deleted)
        # if we dont skip these rows, the returning table has empty rows at the deleted positions
        non_none_rows = [row for row in self.data if any(row)]
        # print using tabulate
        print(tabulate(non_none_rows[:no_of_rows], headers=headers)+'\n')


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

        self.__dict__.update(tmp_dict)

