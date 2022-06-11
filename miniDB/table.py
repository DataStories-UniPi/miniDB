from __future__ import annotations
from hashlib import new
from itertools import groupby
from multiprocessing import Condition
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
                'column[<,<=,=,>=,>]value' or
                'value[<,<=,=,>=,>]column'.

                Operatores supported: (<,<=,=,>=,>)
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



    def _select_where(self, return_columns, condition=None, group_by_list=None, having=None, order_by=None, top_k=None, distinct=False):
        '''
        Select and return a table containing specified columns and rows where condition is met.

        Args:
            return_columns: list. The columns to be returned.
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.

                Operators supported: (<,<=,==,>=,>)

            group_by: string. The given GROUP BY clause, containing the columns names in which the table will be grouped
            having: string. The condition given in the HAVING clause. The condition has the following format:
                'column[<,<=,==,>=,>]value' or
                'aggregate function (column)[<,<=,==,>=,>]value'
            order_by: list. The columns that signal that the resulting table should be ordered based on them (no order if None).

            top_k: int. An integer that defines the number of rows that will be returned (all rows if None).
            distinct: boolean. If it is 'True' it indicates that the query is "select distinct" and a new function is called to remove duplicate rows
        '''

        # first run 'WHERE'
        # if condition is None, return all rows
        # if not, return the rows with values where condition is met for value
        if condition is not None:
            column_name, operator, value = self._parse_condition(condition)
            column = self.column_by_name(column_name)
            rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
        else:
            rows = [i for i in range(len(self.data))]

        all_columns = [i for i in range(len(self.column_names))]


        # copy the old dict, but only the rows of data
        # with index in rows/columns (the indexes that we want returned)
        # but keep ALL columns
        dict = {(key):([[self.data[i][j] for j in all_columns] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}

        # create Table object
        s_table = Table(load=dict)

        if group_by_list is not None:
            groupby_table = s_table._group_by(group_by_list)


        called_agg = False # indicates if an agg fun is inside the SELECT list
        agg_tables = []

        return_cols = []

        # examine SELECT LIST
        # if '*' return all columns, else find the column indexes for the columns specified
        if return_columns == '*':
            return_cols = [i for i in range(len(self.column_names))]
        else:

            for col in return_columns.split(','):

                if col in self.column_names:
                    if group_by_list is None:
                        return_cols.append(self.column_names.index(col.strip()))
                        if called_agg:
                            raise Exception(f"ERROR: columns in SELECT list must appear in the GROUP BY clause or be used in an aggregate function")
                 
                    else:
                        return_cols.append(groupby_table.column_names.index(col.strip()))

                if(col.startswith("max ") or col.startswith("min ") or col.startswith("avg ") or col.startswith("count ") or col.startswith("sum ")):
                    
                    if group_by_list is None:

                        called_agg = True
                        col = col.split(" ")
                        agg_tables.append(_call_agg(s_table,col[1:],col[0]))

                        if len(return_cols) > 0:
                            raise Exception(f"ERROR: columns in SELECT list must appear in the GROUP BY clause or be used in an aggregate function")
                    else:
                        col = col.split(" ")

                        groupby_table = _call_agg_on_group_by(groupby_table,group_by_list,s_table,col[1:],col[0])
        
                        return_cols.append(len(groupby_table.column_names)-1)

        if group_by_list is not None:
            original = s_table
            s_table = groupby_table

        #concatenate agg_tables ONLY WHEN GROUP BY list is empty!!
        if called_agg and (group_by_list is None):

            t_names = []
            t_types = []
            t_data = []

            for table in agg_tables:
                t_names.append(table.column_names[0])
                t_types.append(table.column_types[0])
                t_data.append(table.data[0][0])

            new_table = Table("temp", t_names, t_types)
            new_table.data = [t_data]

            if having is not None:
                ops = [">=", "<=", "=", ">", "<"]
                for op in ops:
                    splt = having.split(op)
                    if(len(splt)>1):
                        break

                left = splt[0].strip()
                if(left.split(" ")[0] in ["min","max","count","sum","avg"]):

                    agg_column = "agg_"+left.replace(' ', '_')
                    new_condition = "agg_"+left.replace(' ', '_')+ op + splt[1]

                    if agg_column in new_table.column_names:
                        column_name, operator, value = new_table._parse_condition(new_condition)
                        column = new_table.column_by_name(column_name)
                        rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]

                        all_columns = [i for i in range(len(new_table.column_names))]
                        temp_dict = {(key):([[new_table.data[i][j] for j in all_columns] for i in rows] if key=="data" else value) for key,value in new_table.__dict__.items()}
                        new_table = Table(load=temp_dict)
                    else:

                        temp = _call_agg(s_table,left.split(" ")[1:],left.split(" ")[0])
                        new_condition = "agg_"+left.replace(' ', '_')+ op + splt[1]

                        
                        column_name, operator, value = temp._parse_condition(new_condition)
                        column = temp.column_by_name(column_name)
                        rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]

                        if len(rows) == 0:
                            new_table.data = []

                        return new_table
                else:
                    raise Exception(f"Must used agg function here")


            # no need to 'order by' or 'distinct' as the result will always be 1 row
            # just check if a given col is invalid
            if order_by:
                order_cols = order_by.split(',')
                for column in order_cols:
                    if column.split(" ")[0] not in new_table.column_names:
                        raise Exception(f"ERROR:  column '{column}' does not exist")

            return new_table


        if having is not None:

            ops = [">=", "<=", "=", ">", "<"]
            for op in ops:
                splt = having.split(op)
                if(len(splt)>1):
                    break

            # having can be used without GROUP BY
            # right side can have agg fun
            if group_by_list is None:

                right = splt[1].strip()
                new_condition = having

                if(right.split(" ")[0] in ["min","max","count","sum","avg"]):
                    temp = _call_agg(s_table, right.split(" ")[1:],right.split(" ")[0])
                    new_condition = str(splt[0])+str(op)+str(int(temp.data[0][0]))

            else:
                left = splt[0].strip()
                if(left.split(" ")[0] in ["min","max","count","sum","avg"]):

                    agg_table = "agg_"+left.replace(' ', '_')
                    new_condition = "agg_"+left.replace(' ', '_')+ op + splt[1]

                    if agg_table not in s_table.column_names:
                        groupby_table = _call_agg_on_group_by(groupby_table,group_by_list,original,left.split(" ")[1:],left.split(" ")[0])
                else:
                    new_condition = having

            # do the same as WHERE only now with the string 'having'
            column_name, operator, value = s_table._parse_condition(new_condition)
            column = s_table.column_by_name(column_name)
            rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]

            all_columns = [i for i in range(len(s_table.column_names))]
            temp_dict = {(key):([[s_table.data[i][j] for j in all_columns] for i in rows] if key=="data" else value) for key,value in s_table.__dict__.items()}
            s_table = Table(load=temp_dict)


        # if the query has 'order by' without 'distinct'
        # order by is applied on the table with all the columns
        if order_by and not(distinct):
            order_cols = order_by.split(',')
            s_table.order_by(order_cols)

        # create new dict from the s_table object that has only the columns that will be displayed
        # (only the columns in SELECT)
        new_dict = {(key):([[s_table.data[i][j] for j in return_cols] for i in range(len(s_table.data))] if key=="data" else value) for key,value in s_table.__dict__.items()}

        # we need to set the new column names/types and no of columns, since we might
        # only return some columns
        new_dict['column_names'] = [s_table.column_names[i] for i in return_cols]
        new_dict['column_types'] = [s_table.column_types[i] for i in return_cols]

        # create the new table object to be returned
        s_table = Table(load=new_dict)


        if(distinct == True):
            s_table.distinct()

            # if the query has 'order by' AND 'distinct', the 'order by' action is called AFTER the
            # 'distinct' function and the table has only the rows that will be displayed

            # NOTE : for SELECT DISTINCT, ORDER BY expressions must appear in select list
            if order_by:
                order_cols = order_by.split(',')
                s_table.order_by(order_cols)

        # if needed, keep only top k rows
        s_table.data = s_table.data[:int(top_k)] if isinstance(top_k,str) else s_table.data

        return s_table


    def _select_where_with_btree(self, return_columns, bt, condition, distinct=False, order_by=None, desc=True, top_k=None):

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

        s_table.data = list(set(map(lambda x: tuple(x), s_table.data))) if distinct else s_table.data

        if order_by and not(distinct):
            order_cols = order_by.split(',')
            s_table.order_by(order_cols)

        s_table.data = s_table.data[:int(top_k)] if isinstance(top_k,str) else s_table.data

        return s_table


    def order_by(self, column_names):
        '''
        Order table based on column.

        Args:
            column_names: list of strings. Names of columns in the order given by the user.
            Also the user can give the keyword ASC|DESC after the name of each table

        This function works just like SQL's ORDER BY

        e.g.:  ORDER BY column1 ASC|DESC , column2 ASC|DESC

        so you can have a query like this:
        ...ORDER BY Country ASC, CustomerName DESC

        This means that the returning table is sorted first with the 'Country' with Ascending order
        and if there are rows with the same 'Country', they are sorted according to 'CustomerName'
        with descending order. The same is done if there are more than 2 columns in the 'ORDER BY'.

        '''
        target_cols = []        # will append here the indexes of the columns that will be sorted
                                # in the order given in ORDER BY

        target_cols_order = []  # will append True or False for each column to indicate ASC or DESC
                                # default is False

        for col in column_names:

            input_col = (col.strip()).split(" ")
            if(len(input_col)>2): # can only have 2 keywords : columnName ASC|DESC
                raise Exception("Syntax error")

            # add table index
            target_cols.append(list(self.column_names).index(input_col[0]))

            # add the boolean
            if(len(input_col)==2):
                if(input_col[1]=='desc'):
                    target_cols_order.append(True)
                elif(input_col[1]=='asc'):
                    target_cols_order.append(False)

            # the query didn't have ASC|DESC, default is False
            else:
                target_cols_order.append(False)

        # function will sort the given self.data
        self._sort(self.data,0,len(self.data),target_cols,target_cols_order)


    def _sort(self,input_list,indexStart,indexEnd,columns,reverses):
        '''
        Args:

        input_list: list. the list that will be sorted.
        indexStart: int. The index of an element of the input_list (see below).
        indexEnd: int. The index of an element of the input_list (see below).
        columns: list of ints. Contains the indexes of the columns that will be sorted.
        reverses: list of booleans - contains the desired orders in which each column
        given in 'columns' will be sorted (True = desc and False = asc).

        Important: The indexes in 'columns' are given in the order the columns will 
        be sorted (in the same order they are given in the ORDER BY clause).

        A sublist is created from 'indexStart' till 'indexEnd' and is sorted.
        Then it is checked for duplicates. For each 'block' of duplicates do a recursive
        call to sort it according to the next column. This is done by passing in the arguments columns[1:]
        and reverses[1:], in order for the function to sort on the next column.

        Returns:
        input_list with elements between the indexes 'indexStart' 'indexEnd' sorted
        '''
        if(indexStart == indexEnd):
            return input_list

        # make copy of 'input_list' from index 'indexStart' till 'indexEnd'
        # sort according to the first column given in the 'columns' list,
        # with asc/desc boolean also from the first element of the 'reverses' list
        input_list_copy = sorted(input_list[indexStart:indexEnd+1],key=lambda x: (x[columns[0]]),reverse=reverses[0])

        # initialize variables for the loop
        prev = input_list_copy[0][columns[0]]
        initial = 0

        for i in range(len(input_list_copy)):

            if input_list_copy[i][columns[0]] == prev:

                if(i== len(input_list_copy)-1):
                    # print(f" found {prev} from {initial} till {i}")

                    # The duplicates are [initial,i], rec call _sort for the next column
                    if(len(columns)>1):
                        input_list_copy = self._sort(input_list_copy,initial,i,columns[1:],reverses[1:])

            else:
                # print(f" found {prev} from {initial} till {i-1}")

                # The duplicates are [initial,i-1], rec call _sort again
                if(len(columns)>1):
                    input_list_copy = self._sort(input_list_copy,initial,i-1,columns[1:],reverses[1:])

                prev = input_list_copy[i][columns[0]]
                initial = i

        input_list[indexStart:indexEnd+1] = input_list_copy

        return input_list


    def _group_by(self,group_by_list):
        '''
        Args:
        groups: the string given in the query, containing the GROUP BY list

        Returns:
        a table object with the columns given in the GROUP BY clause (groups)
        with distinct rows
        '''

        if(group_by_list is None):
            return

        # use the _select_where function to do a 'query': select distinct <groups> from <self>
        s_table = self._select_where(group_by_list, distinct = True)

        return s_table


    def distinct(self):
        '''
        Remove duplicate rows in the table object

        The function first checks if the PK is in the columns. If so, it immediately stops,
        since the rows are guaranted to be distinct if the PK column is present

        If the PK is not in the columns, the function sorts the entire table
        by calling the 'order_by' function and giving it all the columns.
        Then it does a simple loop to remove duplicates (since it is sorted, duplicates will be neighbouring).
        '''
        if(self.pk in self.column_names):
            return

        self.order_by(self.column_names)

        prev = self.data[0]

        for elem in list(self.data[1:]):
            if (elem == prev):
                self.data.remove(elem)
            else:
                prev = elem


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


def _call_agg(table, row, aggtype):

    # helping dict
    agg_funs = {'min':_min,
                'max':_max,
                'avg':_avg,
                'count':_count,
                'sum':_sum}

    distinct = False
    col = row[0]

    if row[0] == "distinct":
        distinct = True
        col = row[1]

    s_table =  table._select_where(return_columns = col)

    raw_data  = []

    for d in s_table.data:
        raw_data.append(d[0])

    if distinct:
        c_names = ["agg_" + aggtype +"_distinct_" + col.replace(' ', '_')]
    else:
        c_names = ["agg_" + aggtype +"_" + col.replace(' ', '_')]
    c_types = s_table.column_types
    n_table = Table("temp", c_names, c_types)
    n_table.data = [[agg_funs[aggtype](raw_data,distinct)]]

    return n_table


def _call_agg_on_group_by(group_by_table, group_by_list, orignal_table, target_col, agg_fun_type):

    # first get the names of the 'groups'
    temp_table = group_by_table._select_where(return_columns=group_by_list)

    group_names  = []

    for d in temp_table.data:
        group_names.append(d[0])

    new_column =[]

    for group in group_names:

        condition = group_by_list + "=" + group

        tg = target_col[0]
        if len(target_col)==2:
            tg = target_col[1]

        agg_table = orignal_table._select_where(return_columns=tg, condition=condition )

        agg2 = _call_agg(agg_table,target_col,agg_fun_type)

        new_column.append(agg2.data[0][0])

    group_by_table.column_names.append(agg2.column_names[0])
    group_by_table.column_types.append(agg2.column_types[0])

    for i in range(len(group_by_table.data)):
        (group_by_table.data[i]).append(new_column[i])

    return group_by_table


def _max(rows,distinct):
    return max(rows)

def _min(rows,distinct):
    return min(rows)

def _sum(rows,distinct):
    if(distinct):
        rows = list(dict.fromkeys(rows)) # remove duplicates
    return sum(rows)

def _count(rows,distinct):
    if(distinct):
        rows = list(dict.fromkeys(rows)) # remove duplicates
    return len(rows)

def _avg(rows,distinct):
    if(distinct):
        rows = list(dict.fromkeys(rows)) # remove duplicates
    return sum(rows) / len(rows)
