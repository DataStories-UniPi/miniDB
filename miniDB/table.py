from __future__ import annotations
from tabulate import tabulate
import pickle
import os
import sys
import re
import copy

sys.path.append(f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/miniDB')

from misc import get_op, split_condition, split_complex_conditions
from Extendible_Hash import Hash


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
    def __init__(self, name=None, column_names=None, column_types=None, primary_key=None, unique=None, load=None):
        # unique parameter is a list with the names of the unique columns

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

            if unique is not None:
                self.unique = unique
            else:
                self.unique = []
                
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
        
        # make a condition with the unique columns to search for rows that have same unique values as the row we are inserting            
        if len(self.unique) != 0:
            condition_4_unique = self.unique[0] + '=' + row[self.column_names.index(self.unique[0])]
            for i in range(1,len(self.unique)):
                condition_4_unique += ' or ' + self.unique[i] + '=' +  row[self.column_names.index(self.unique[i])]
                
            temp_table = self._select_where('*',condition_4_unique)
            if len(temp_table.data)!=0: # if we find a row with the same unique value then give error
                raise ValueError(f'ERROR -> Cannot insert {row} values. The following row with same unique columns already exists:\n{temp_table.data[0]}')

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

            # if value is to be appended to the primary_key column, check that it doesnt alrady exist (no duplicate primary keys)
            if i==self.pk_idx and row[i] in self.column_by_name(self.pk):
                raise ValueError(f'## ERROR -> Value {row[i]} already exists in primary key column.')
            elif i==self.pk_idx and row[i] is None:
                raise ValueError(f'ERROR -> The value of the primary key cannot be None.')
            
            


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
            condition: string. Either complex or simple.
                A simple condition has the following format:
                '(not) column[<,<=,==,>=,>]value' or
                '(not) value[<,<=,==,>=,>]column'.
                Complex conditions consist of simple ones connected with operators 'or','and' and 'not'
                
                Operatores supported: (<,<=,==,>=,>,between,and,or,not)
        '''
        rows = self._rows_that_satisfy_complex_condition(condition)            

        # get the set column                  
        set_column_idx = self.column_names.index(set_column)
        # set_columns_indx = [self.column_names.index(set_column_name) for set_column_name in set_column_names]               

        # for each row, if all conditions are met in the row, replace it with set_value
        for i in rows:
            self.data[i][set_column_idx] = set_value


        # self._update()
                # print(f"Updated {len(indexes_to_del)} rows")


    def _delete_where(self, condition):
        '''
        Deletes rows where condition is met.

        Important: delete replaces the rows to be deleted with rows filled with Nones.
        These rows are then appended to the insert_stack.

        Args:
            condition: string. Either complex or simple.
                A simple condition has the following format:
                '(not) column[<,<=,==,>=,>]value' or
                '(not) value[<,<=,==,>=,>]column'.
                Complex conditions consist of simple ones connected with operators 'or','and' and 'not'
                
                Operatores supported: (<,<=,==,>=,>,between,and,or,not)
        '''


        indexes_to_del = self._rows_that_satisfy_complex_condition(condition)




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
            condition: string. Either complex or simple.
                A simple condition has the following format:
                '(not) column[<,<=,==,>=,>]value' or
                '(not) value[<,<=,==,>=,>]column'.
                Complex conditions consist of simple ones connected with operators 'or','and' and 'not'
                
                Operatores supported: (<,<=,==,>=,>,between,and,or,not)
            distinct: boolean. If True, the resulting table will contain only unique rows (False by default).
            order_by: string. A column name that signals that the resulting table should be ordered based on it (no order if None).
            desc: boolean. If True, order_by will return results in descending order (False by default).
            limit: int. An integer that defines the number of rows that will be returned (all rows if None).
        '''

        # if * return all columns, else find the column indexes for the columns specified
        if return_columns == '*':
            return_cols = [i for i in range(len(self.column_names))]
        else:
            return_cols = [self.column_names.index(col.strip()) for col in return_columns.split(',')]

        # if condition is None, return all rows
        # if not, return the rows with values where condition is met for value
        if condition is not None:
            rows = self._rows_that_satisfy_complex_condition(condition)            

        else:
            rows = [i for i in range(len(self.data))]

        # copy the old dict, but only the rows and columns of data with index in rows/columns (the indexes that we want returned)
        dict = {(key):([[self.data[i][j] for j in return_cols] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}

        # we need to set the new column names/types and no of columns, since we might
        # only return some columns
        dict['column_names'] = [self.column_names[i] for i in return_cols]
        dict['column_types']   = [self.column_types[i] for i in return_cols]

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
        if isinstance(limit,str):
            s_table.data = [row for row in s_table.data if any(row)][:int(limit)]

        return s_table


    def _select_where_with_btree(self, return_columns, bt, condition, distinct=False, order_by=None, desc=True, limit=None):

        # if * return all columns, else find the column indexes for the columns specified
        if return_columns == '*':
            return_cols = [i for i in range(len(self.column_names))]
        else:
            return_cols = [self.column_names.index(colname) for colname in return_columns]


        column_name, operator, value = self._parse_condition(condition)

        # if the column in condition is not a primary key, abort the select
        if column_name not in self.unique:
            print('Column is not unique. Aborting')

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

    def _select_where_with_hash(self, return_columns, hash, condition):
        '''
        Is called by database to perform search based on hash index
        '''

        # if * return all columns, else find the column indexes for the columns specified
        if return_columns == '*':
            return_cols = [i for i in range(len(self.column_names))]
        else:
            return_cols = [self.column_names.index(colname) for colname in return_columns]

        column_name, operator, value = self._parse_condition(condition)

        row = hash.find(value)

        dict = {(key):([[self.data[row][j] for j in return_cols]] if key=="data" else value) for key,value in self.__dict__.items()}



        dict['column_names'] = [self.column_names[i] for i in return_cols]
        dict['column_types']   = [self.column_types[i] for i in return_cols]

        s_table = Table(load=dict)

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
            condition: string. Either complex or simple.
                A simple condition has the following format:
                '(not) column[<,<=,==,>=,>]value' or
                '(not) value[<,<=,==,>=,>]column'.
                Complex conditions consist of simple ones connected with operators 'or','and' and 'not'
                
                Operatores supported: (<,<=,==,>=,>,between,and,or,not)
        '''
        # get columns and operator
        broken_complex_condition = split_complex_conditions(condition)
        for i in range(len(broken_complex_condition)):
            if broken_complex_condition[i]!='and' and broken_complex_condition[i]!='or':
                column_name_left, operator, column_name_right = self._parse_condition(broken_complex_condition[i], join=True)
                broken_complex_condition[i] = [column_name_left, operator, column_name_right]                        


        # finding conditions that have 2 columns from both tables, or else we give error
        condition_with_2_columns = []
        for i in broken_complex_condition:
            if isinstance(i,list) and i[0] in self.column_names and i[2] in table_right.column_names: # checking all operators in condition to make sure they are '='
                condition_with_2_columns.append(broken_complex_condition.index(i))

        if len(condition_with_2_columns) == 0:
            raise Exception(f'There is no condition that uses both left and right table Columns.\nOne such condition is needed to connect the 2 tables.\n Left Table Columns: {self.column_names}.\nRight Table Columns: {table_right.column_names}')


        # if a condition doesn't campare 2 columns but it compares a column and a value, parsing has been done incorectly because the types will be incorrect
        # parse_condition has been called for join, we need to change the type of the values in the parsed conditions
        for i in range(len(broken_complex_condition)):
            if broken_complex_condition[i]!='and' and broken_complex_condition[i]!='or' and i not in condition_with_2_columns:
                if broken_complex_condition[i][0] in table_right.column_names: # if the column before the operator is from the right table, change the type of the variable after the operator                        
                    coltype = table_right.column_types[table_right.column_names.index(broken_complex_condition[i][0])] # get the type of the column before the operator
                    broken_complex_condition[i][2] = coltype(broken_complex_condition[i][2]) # change the variable to its correct type
                elif broken_complex_condition[i][0] in self.column_names:
                    coltype = self.column_types[self.column_names.index(broken_complex_condition[i][0])] # get the type of the column before the operator
                    broken_complex_condition[i][2] = coltype(broken_complex_condition[i][2]) # change the variable to its correct type     


        
        if join_type in ['left','right','full']:
            # check if any of the above conditions  has '=' operator. Those can be used for sure
            can_be_used_for_join = []
            for i in condition_with_2_columns:
                if isinstance(broken_complex_condition[i],list) and broken_complex_condition[i][1]=='=': 
                    can_be_used_for_join.append(i)

            if (len(can_be_used_for_join)==0):
                class CustomFailException(Exception):
                    pass
                raise CustomFailException('Outer Joins can only be used if the condition operator is "=".\n')
        else:
            can_be_used_for_join = condition_with_2_columns        



        # get the column names of both tables with the table name in front
        # ex. for left -> name becomes left_table_name_name etc
        left_names = [f'{self._name}.{colname}' if self._name!='' else colname for colname in self.column_names]
        right_names = [f'{table_right._name}.{colname}' if table_right._name!='' else colname for colname in table_right.column_names]

        # define the new tables name, its column names and types
        join_table_name = ''
        join_table_colnames = left_names+right_names
        join_table_coltypes = self.column_types+table_right.column_types
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)

        return join_table, broken_complex_condition, can_be_used_for_join 


    def _inner_join(self, table_right: Table, condition):
        '''
        Join table (left) with a supplied table (right) where condition is met.

        Args:
            condition: string. Either complex or simple.
                A simple condition has the following format:
                '(not) column[<,<=,==,>=,>]value' or
                '(not) value[<,<=,==,>=,>]column'.
                Complex conditions consist of simple ones connected with operators 'or','and' and 'not'
                
                Operatores supported: (<,<=,==,>=,>,between,and,or,not)
        '''
        join_table, broken_complex_condition, conditions_that_can_be_used_for_join = self._general_join_processing(table_right, condition, 'inner')

        # count the number of operations (<,> etc)
        no_of_ops = 0
        # this code is dumb on purpose... it needs to illustrate the underline technique
        # for each value in left column and right column, if condition, append the corresponding row to the new table
        for row_left in self.data:

            # temp is same as broken_complex_condition, but we'll change the columns with the corresponding values each time we loop
            temp = copy.deepcopy(broken_complex_condition)

            for i in range(len(temp)):
                if isinstance(temp[i],list): # if current item isn't 'and' or 'or'
                    if temp[i][0] in self.column_names: # if item before the operator is column from this table
                        temp[i][0] = row_left[self.column_names.index(temp[i][0])] # replace with the value
                        

            for row_right in table_right.data:

                # make temp2 to replace values of columns of the right table. we need previous temp for next iterations so we can't use it
                temp2 = copy.deepcopy(temp)

                for i in range(len(temp2)):
                    if isinstance(temp2[i],list): # if current item isn't 'and' or 'or'
                        if i in conditions_that_can_be_used_for_join: # if the condition we ar checking is used to join the 2 tables then replace the item after operator
                            temp2[i][2]  = row_right[table_right.column_names.index(temp2[i][2])]
                        elif temp2[i][0] in table_right.column_names: # else check if we can replace the item before the operator
                            temp2[i][0] = row_right[table_right.column_names.index(temp2[i][0])] 

                
                if any((isinstance(item,list) and item[0] is None and item[2] is None) for item in temp2): 
                    continue
                no_of_ops+=1
                
                include_row = get_op(temp2[0][1], temp2[0][0], temp2[0][2]) # now temp2 has only values. We can check if we should add this column by checking all conditions in temp2
                temp2.pop(0)
                while len(temp2) != 0:
                    if temp2[0] == 'and':
                        include_row = include_row and get_op(temp2[1][1], temp2[1][0], temp2[1][2])
                    elif temp2[0] == 'or':
                        include_row = include_row or get_op(temp2[1][1], temp2[1][0], temp2[1][2])
                    temp2.pop(0) # pop the 'and' or 'or' operator
                    temp2.pop(0) # pop the condition checked

                if include_row: #EQ_OP
                    join_table._insert(row_left+row_right)

        return join_table
    
    def _left_join(self, table_right: Table, condition):
        '''
        Perform a left join on the table with the supplied table (right).

        Args:
            condition: string. Either complex or simple.
                A simple condition has the following format:
                '(not) column[<,<=,==,>=,>]value' or
                '(not) value[<,<=,==,>=,>]column'.
                Complex conditions consist of simple ones connected with operators 'or','and' and 'not'
                
                Operatores supported: (<,<=,==,>=,>,between,and,or,not)
        '''
        join_table, broken_complex_condition, conditions_that_can_be_used_for_join = self._general_join_processing(table_right, condition, 'left')


        right_table_row_length = len(table_right.column_names)

        for row_left in self.data:

            # temp is same as broken_complex_condition, but we'll change the columns with the corresponding values each time we loop
            temp = copy.deepcopy(broken_complex_condition)
            found_null_value = False
            for i in range(len(temp)):
                if isinstance(temp[i],list): # if current item isn't 'and' or 'or'
                    if temp[i][0] is None:
                        found_null_value = True
                    if temp[i][0] in self.column_names: # if item before the operator is column from this table
                        temp[i][0] = row_left[self.column_names.index(temp[i][0])] # replace with the value


            if found_null_value: # if any value needed to check the condition is null in the left table
                continue # go to the next one

            no_match_found = True 
            # for each row in the right, if it satisfies the condition, add it to the join table 
            for row_right in table_right.data:
                    
                    # make temp2 to replace values of columns of the right table. we need previous temp for next iterations so we can't use it
                    temp2 = copy.deepcopy(temp)
                    for i in range(len(temp2)):
                        if isinstance(temp2[i],list): # if current item isn't 'and' or 'or'
                            if i in conditions_that_can_be_used_for_join: # if the condition we ar checking is used to join the 2 tables then replace the item after operator
                                temp2[i][2]  = row_right[table_right.column_names.index(temp2[i][2])]
                            elif temp2[i][0] in table_right.column_names: # else check if we can replace the item before the operator
                                temp2[i][0] = row_right[table_right.column_names.index(temp2[i][0])] 


                    include_row = get_op(temp2[0][1], temp2[0][0], temp2[0][2]) # now temp2 has only values. We can check if we should add this column by checking all conditions in temp2
                    temp2.pop(0)
                    while len(temp2) != 0:
                        if temp2[0] == 'and':
                            include_row = include_row and get_op(temp2[1][1], temp2[1][0], temp2[1][2])
                        elif temp2[0] == 'or':
                            include_row = include_row or get_op(temp2[1][1], temp2[1][0], temp2[1][2])
                        temp2.pop(0) # pop the 'and' or 'or' operator
                        temp2.pop(0) # pop the condition checked

                    if include_row: #EQ_OP
                        join_table._insert(row_left + row_right)
                        no_match_found = False                    
                        

            if no_match_found:
                join_table._insert(row_left + right_table_row_length*["NULL"])
                

        return join_table

    def _right_join(self, table_right: Table, condition):
        '''
        Perform a right join on the table with the supplied table (right).

        Args:
            condition: string. Either complex or simple.
                A simple condition has the following format:
                '(not) column[<,<=,==,>=,>]value' or
                '(not) value[<,<=,==,>=,>]column'.
                Complex conditions consist of simple ones connected with operators 'or','and' and 'not'
                
                Operatores supported: (<,<=,==,>=,>,between,and,or,not)
        '''
        join_table, broken_complex_condition, conditions_that_can_be_used_for_join = self._general_join_processing(table_right, condition, 'right')

        left_table_row_length = len(self.column_names)

        for row_right in table_right.data:

            # temp is same as broken_complex_condition, but we'll change the columns with the corresponding values each time we loop
            temp = copy.deepcopy(broken_complex_condition)
            found_null_value = False
            for i in range(len(temp)):
                if isinstance(temp[i],list): # if current item isn't 'and' or 'or'
                    if i in conditions_that_can_be_used_for_join: # if the condition we ar checking is used to join the 2 tables then replace the item after operator
                        temp[i][2]  = row_right[table_right.column_names.index(temp[i][2])]
                        if temp[i][2] is None:
                            continue
                    elif temp[i][0] in table_right.column_names: # else check if we can replace the item before the operator
                        temp[i][0] = row_right[self.column_names.index(temp[i][0])]
                        if temp[i][0] is None:
                            continue


            if found_null_value: # if any value needed to check the condition is null in the left table
                continue # go to the next one


            no_match_found = True 
            # for each row in the left, if it satisfies the condition, add it to the join table 
            for row_left in self.data:

                
                # make temp2 to replace values of columns of the right table. we need previous temp for next iterations so we can't use it
                temp2 = copy.deepcopy(temp)
                for i in range(len(temp2)):
                    if isinstance(temp2[i],list): # if current item isn't 'and' or 'or'
                        if temp2[i][0] in self.column_names:
                            temp2[i][0] = row_left[self.column_names.index(temp2[i][0])]


                include_row = get_op(temp2[0][1], temp2[0][0], temp2[0][2]) # now temp2 has only values. We can check if we should add this column by checking all conditions in temp2
                temp2.pop(0)
                while len(temp2) != 0:
                    if temp2[0] == 'and':
                        include_row = include_row and get_op(temp2[1][1], temp2[1][0], temp2[1][2])
                    elif temp2[0] == 'or':
                        include_row = include_row or get_op(temp2[1][1], temp2[1][0], temp2[1][2])
                    temp2.pop(0) # pop the 'and' or 'or' operator
                    temp2.pop(0) # pop the condition checked

                if include_row: #EQ_OP
                    join_table._insert(row_left + row_right)
                    no_match_found = False                
                        
                        
            if no_match_found:
                join_table._insert(left_table_row_length*["NULL"] + row_right)

        return join_table
    
    def _full_join(self, table_right: Table, condition):
        '''
        Perform a full join on the table with the supplied table (right).

        Args:
            condition: string. Either complex or simple.
                A simple condition has the following format:
                '(not) column[<,<=,==,>=,>]value' or
                '(not) value[<,<=,==,>=,>]column'.
                Complex conditions consist of simple ones connected with operators 'or','and' and 'not'
                
                Operatores supported: (<,<=,==,>=,>,between,and,or,not)
        '''

        join_table, broken_complex_condition, conditions_that_can_be_used_for_join = self._general_join_processing(table_right, condition, 'full')
        
        right_table_row_length = len(table_right.column_names)
        left_table_row_length = len(self.column_names)

        right_rows_added = [] # list with rows from right table that have been matched       
        #insert rows based on left table   
        for row_left in self.data:
            # temp is same as broken_complex_condition, but we'll change the columns with the corresponding values each time we loop
            temp = copy.deepcopy(broken_complex_condition)
            found_null_value = False
            for i in range(len(temp)):
                if isinstance(temp[i],list): # if current item isn't 'and' or 'or'                    
                    if temp[i][0] in self.column_names: # if item before the operator is column from this table
                        temp[i][0] = row_left[self.column_names.index(temp[i][0])] # replace with the value
                    if temp[i][0] is None:
                        found_null_value = True


            if found_null_value: # if any value needed to check the condition is null in the left table
                continue # go to the next one

            no_match_found = True 
            # for each row in the right, if it satisfies the condition, add it to the join table 
            for row_right in table_right.data:
                    
                    # make temp2 to replace values of columns of the right table. we need previous temp for next iterations so we can't use it
                    temp2 = copy.deepcopy(temp)
                    for i in range(len(temp2)):
                        if isinstance(temp2[i],list): # if current item isn't 'and' or 'or'
                            if i in conditions_that_can_be_used_for_join: # if the condition we ar checking is used to join the 2 tables then replace the item after operator
                                temp2[i][2]  = row_right[table_right.column_names.index(temp2[i][2])]
                            elif temp2[i][0] in table_right.column_names: # else check if we can replace the item before the operator
                                temp2[i][0] = row_right[table_right.column_names.index(temp2[i][0])] 


                    include_row = get_op(temp2[0][1], temp2[0][0], temp2[0][2]) # now temp2 has only values. We can check if we should add this column by checking all conditions in temp2
                    temp2.pop(0)
                    while len(temp2) != 0:
                        if temp2[0] == 'and':
                            include_row = include_row and get_op(temp2[1][1], temp2[1][0], temp2[1][2])
                        elif temp2[0] == 'or':
                            include_row = include_row or get_op(temp2[1][1], temp2[1][0], temp2[1][2])
                        temp2.pop(0) # pop the 'and' or 'or' operator
                        temp2.pop(0) # pop the condition checked

                    if include_row: #EQ_OP
                        join_table._insert(row_left + row_right)
                        right_rows_added.append(table_right.data.index(row_right))
                        no_match_found = False                    
                        

            if no_match_found:
                join_table._insert(row_left + right_table_row_length*["NULL"])



        # iterate right table and check if any of the rows have not been added using right_rows_added list
        for row_right in range(len(table_right.data)):
            if row_right not in right_rows_added and all(item is not None for item in table_right.data[row_right]):
                join_table._insert(left_table_row_length*["NULL"] + table_right.data[row_right])            


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
                '(not) column[<,<=,==,>=,>,between]value' or
                '(not) value[<,<=,==,>=,>,between]column'.
                
                Operatores supported: (<,<=,==,>=,>,between,not)
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

        
        
        # if operator is between (or between with NOT operator), then right will not be one value, but a list 2 values representing the range
        if (op=='between' or op=='not_between'):
            return left, op, [coltype(right[0]), coltype(right[1])]

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



    def _rows_that_satisfy_complex_condition(self, condition):
        '''
        Returns the rows where condition is met in a list. condition can be either complex or simple.

        Args:
            condition: string. Either complex or simple.
                A simple condition has the following format:
                '(not) column[<,<=,==,>=,>]value' or
                '(not) value[<,<=,==,>=,>]column'.
                Complex conditions consist of simple ones connected with operators 'or','and' and 'not'
                
                Operatores supported: (<,<=,==,>=,>,between,and,or,not)
                
        '''
        # break the complex condition into smaller ones and put them into a list
        broken_complex_condition = split_complex_conditions(condition)


        columns = {} # dict with columns in conditions (to get the columns of the conditions )


        
        for i in range(len(broken_complex_condition)):
            if (broken_complex_condition[i]!='and') and (broken_complex_condition[i]!='or'): # for each condition

                column_name, operator, value = self._parse_condition(broken_complex_condition[i]) # parse each condition seperately
                broken_complex_condition[i] = [column_name,operator,value]

                if (not column_name in columns): # if this column hasn't been obtained for previous condition
                    columns[column_name] = self.column_by_name(column_name)
                    # get the column and add it in the dictionary


        

        rows = []


        # for each row, if all conditions are met in the row, add it to indexes_to_del
        for row_ind, value_of_columns in enumerate(list(columns.values())[0]):

            meets_all_conditions = True # true if all conditions are met on this row

            # checking each condition
            for i in range(len(broken_complex_condition)):
                if (broken_complex_condition[i]!='and') and (broken_complex_condition[i]!='or'): # if we are currently checking condition
                    
                    # get column_value, operator and value
                    column_of_condition = broken_complex_condition[i][0] # (this is just the column name)
                    operator = broken_complex_condition[i][1]
                    value = broken_complex_condition[i][2]

                    column_value = columns[column_of_condition][row_ind] # get the column value using the dictionary we created


                    current_condition = get_op(operator, column_value, value) # check if current condition is valid

                    if i==0 or broken_complex_condition[i-1]=='and': # for the first condition or the conditions that are connected with 'and' operator
                        meets_all_conditions = meets_all_conditions and current_condition
                    elif broken_complex_condition[i-1]=='or': # if non-first condition where previous operator is 'or'
                        meets_all_conditions = meets_all_conditions or current_condition

                
            if meets_all_conditions:
                rows.append(row_ind)
        
        return rows