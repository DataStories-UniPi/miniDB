from __future__ import annotations
from hashlib import new
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


    def _select_where(self, return_columns, condition=None, group_by=None, having=None, order_by=None, top_k=None, distinct=False):
        '''
        Select and return a table containing specified columns and rows where condition is met.

        Args:
            return_columns: list. The columns to be returned.
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.

                Operatores supported: (<,<=,==,>=,>)
            group_by: string. The given GROUP BY clause, containig the columns names in which the table will be grouped
            having: string. The condition given in the HAVING clause. The condition has the following format:
                'column[<,<=,==,>=,>]value' or
                'aggregate function (column)[<,<=,==,>=,>]value'
            order_by: list. The columns that signal that the resulting table should be ordered based on them (no order if None).
            top_k: int. An integer that defines the number of rows that will be returned (all rows if None).
            distinct: boolean. If it is 'True' it indicates that the query is "select distinct" and a new function is called to remove duplicate rows
        
        The function behaves differntly if group_by is None or not.
        If group_by is None the procedure is almost vanilla.
        
        
        '''


        if group_by is not None:

            # first run WHERE clause by checking the given condition to create the s_table object
            if condition is not None:
                column_name, operator, value = self._parse_condition(condition)
                column = self.column_by_name(column_name)
                rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
            else:
                rows = [i for i in range(len(self.data))]

            all_columns = [i for i in range(len(self.column_names))]
            dict = {(key):([[self.data[i][j] for j in all_columns] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}

            s_table = Table(load=dict)

            # pass the s_table object to group_by function to create
            grouped = s_table.group_by(group_by)
            #
            column_names = grouped.column_names.copy()


            #
            #
            #

            return_cols = []

            if return_columns == '*':
                raise Exception("Syntax error: cannot have '*' in select list when using GROUP BY")
            else:

                '''
                
                
                '''

                for col in return_columns.split(','):

                    if(col.strip() in grouped.column_names):
                        return_cols.append(grouped.column_names.index(col.strip()))

                    elif(col.strip().startswith('min ')):

                        target_column = col.strip()[col.strip().find('(')+1:col.strip().find(')')]

                        grouped = min(original=self,grouped=grouped,target_column=target_column.strip(),column_names=column_names)
                        return_cols.append(grouped.column_names.index('agg_min_' + target_column.strip().replace(' ', '_')))


                    elif(col.strip().startswith('count ')):

                        target_column = col.strip()[col.strip().find('(')+1:col.strip().find(')')]

                        grouped = count(original=self,grouped=grouped,target_column=target_column.strip(),column_names=column_names)
                        return_cols.append(grouped.column_names.index('agg_count_' + target_column.strip().replace(' ', '_')))


                    elif(col.strip().startswith('sum ')):

                        target_column = col.strip()[col.strip().find('(')+1:col.strip().find(')')]

                        grouped = sum(original=self,grouped=grouped,target_column=target_column.strip(),column_names=column_names)
                        return_cols.append(grouped.column_names.index('agg_sum_' + target_column.strip().replace(' ', '_')))


                    elif(col.strip().startswith('max ')):

                        target_column = col.strip()[col.strip().find('(')+1:col.strip().find(')')]

                        grouped = max(original=self,grouped=grouped,target_column=target_column.strip(),column_names=column_names)
                        return_cols.append(grouped.column_names.index('agg_max_' + target_column.strip().replace(' ', '_')))


                    elif(col.strip().startswith('avg ')):

                        target_column = col.strip()[col.strip().find('(')+1:col.strip().find(')')]

                        grouped = avg(original=self,grouped=grouped,target_column=target_column.strip(),column_names=column_names)
                        return_cols.append(grouped.column_names.index('agg_avg_' + target_column.strip().replace(' ', '_')))



                    else:
                        raise Exception("given select list not in GROUP BY")

            



            if(having is not None):

                '''
                
                
                '''

                if(having.startswith('max ')):
                    table_in_agg = having.strip()[having.strip().find('(')+1:having.strip().find(')')]
                    table_in_agg = table_in_agg.strip()

                    if(('agg_max_'+table_in_agg) in grouped.column_names):
                        having = having[(having.index(')')+1):]
                        having = 'agg_max_'+table_in_agg + having

                    else:
                        grouped = max(original=self,grouped=grouped,target_column=table_in_agg.strip(),column_names=column_names)
                        having = having[(having.index(')')+1):]
                        having = 'agg_max_'+table_in_agg + having

                if(having.startswith('min ')):
                    table_in_agg = having.strip()[having.strip().find('(')+1:having.strip().find(')')]
                    table_in_agg = table_in_agg.strip()

                    if(('agg_min_'+table_in_agg.replace(' ', '_')) in grouped.column_names):
                        having = having[(having.index(')')+1):]
                        having = 'agg_min_'+table_in_agg.replace(' ', '_') + having

                    else:
                        grouped = min(original=self,grouped=grouped,target_column=table_in_agg.strip(),column_names=column_names)
                        having = having[(having.index(')')+1):]
                        having = 'agg_min_'+table_in_agg.replace(' ', '_') + having

                if(having.startswith('count ')):
                    table_in_agg = having.strip()[having.strip().find('(')+1:having.strip().find(')')]
                    table_in_agg = table_in_agg.strip()

                    if(('agg_count_'+table_in_agg.replace(' ', '_')) in grouped.column_names):
                        having = having[(having.index(')')+1):]
                        having = 'agg_count_'+table_in_agg.replace(' ', '_') + having

                    else:
                        grouped = count(original=self,grouped=grouped,target_column=table_in_agg.strip(),column_names=column_names)
                        having = having[(having.index(')')+1):]
                        having = 'agg_count_'+table_in_agg.replace(' ', '_') + having



                if(having.startswith('sum ')):
                    table_in_agg = having.strip()[having.strip().find('(')+1:having.strip().find(')')]
                    table_in_agg = table_in_agg.strip()

                    if(('agg_sum_'+table_in_agg.replace(' ', '_')) in grouped.column_names):
                        having = having[(having.index(')')+1):]
                        having = 'agg_sum_'+table_in_agg.replace(' ', '_') + having

                    else:
                        grouped = sum(original=self,grouped=grouped,target_column=table_in_agg.strip(),column_names=column_names)
                        having = having[(having.index(')')+1):]
                        having = 'agg_sum_'+table_in_agg.replace(' ', '_') + having



                if(having.startswith('avg ')):
                    table_in_agg = having.strip()[having.strip().find('(')+1:having.strip().find(')')]
                    table_in_agg = table_in_agg.strip()

                    if(('agg_avg_'+table_in_agg.replace(' ', '_')) in grouped.column_names):
                        having = having[(having.index(')')+1):]
                        having = 'agg_avg_'+table_in_agg.replace(' ', '_') + having

                    else:
                        grouped = avg(original=self,grouped=grouped,target_column=table_in_agg.strip(),column_names=column_names)
                        having = having[(having.index(')')+1):]
                        having = 'agg_avg_'+table_in_agg.replace(' ', '_') + having


                #
                column_name, operator, value = grouped._parse_condition(having)
                column = grouped.column_by_name(column_name)
                rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
            else:
                rows = [i for i in range(len(grouped.data))]


            all_columns = [i for i in range(len(grouped.column_names))]

            temp_dict = {(key):([[grouped.data[i][j] for j in all_columns] for i in rows] if key=="data" else value) for key,value in grouped.__dict__.items()}

            temp_table = Table(load=temp_dict)

            # the rest is the same with the case without GROUP BY

            # if the query has 'order by' without 'distinct'
            # order by is applied on the table with all the columns
            if order_by and not(distinct):
                order_cols = order_by.split(',')
                temp_table.order_by(order_cols)

            return_dict = {(key):([[temp_table.data[i][j] for j in return_cols] for i in range(len(temp_table.data))] if key=="data" else value) for key,value in temp_table.__dict__.items()}

            return_dict['column_names'] = [temp_table.column_names[i] for i in return_cols]
            return_dict['column_types'] = [temp_table.column_types[i] for i in return_cols]

            return_table = Table(load=return_dict)

            if(distinct == True):
                return_table.distinct()

                # if the query has 'order by' AND 'distinct', the 'order by' action is called AFTER the
                # 'distinct' function and the table has only the rows that will be displayed

                # NOTE : for SELECT DISTINCT, ORDER BY expressions must appear in select list
                if order_by:
                    order_cols = order_by.split(',')
                    return_table.order_by(order_cols)

            # if needed, keep only top k rows
            return_table.data = return_table.data[:int(top_k)] if isinstance(top_k,str) else return_table.data



            return return_table

        else:

            # if * return all columns, else find the column indexes for the columns specified
            if return_columns == '*':
                return_cols = [i for i in range(len(self.column_names))]
            else:
                return_cols = [self.column_names.index(col.strip()) for col in return_columns.split(',')]

            all_columns = [i for i in range(len(self.column_names))]

            # if condition is None, return all rows
            # if not, return the rows with values where condition is met for value
            if condition is not None:
                column_name, operator, value = self._parse_condition(condition)
                column = self.column_by_name(column_name)
                rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
            else:
                rows = [i for i in range(len(self.data))]


            # copy the old dict, but only the rows of data
            # with index in rows/columns (the indexes that we want returned)
            # but keep ALL columns
            dict = {(key):([[self.data[i][j] for j in all_columns] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}

            # create Table object
            s_table = Table(load=dict)


            # if the query has 'order by' without 'distinct'
            # order by is applied on the table with all the columns
            if order_by and not(distinct):
                order_cols = order_by.split(',')
                s_table.order_by(order_cols)

            # this check is done to prevent a mistake : new_dict raises exception when the meta_insert_stack is called
            # TODO check why this happens
            if(s_table._name.startswith("meta_insert_stack")):
                return s_table

            # create new dict from the s_table object that has only the columns that will be displayed
            # (only the columns in SELECT)
            new_dict = {(key):([[s_table.data[i][j] for j in return_cols] for i in range(len(s_table.data))] if key=="data" else value) for key,value in s_table.__dict__.items()}

            # we need to set the new column names/types and no of columns, since we might
            # only return some columns
            new_dict['column_names'] = [self.column_names[i] for i in return_cols]
            new_dict['column_types'] = [self.column_types[i] for i in return_cols]

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


    def _select_where_with_btree(self, return_columns, bt, condition, order_by=None, desc=True, top_k=None):

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

        return s_table

    def order_by(self, column_names):
        '''
        Order table based on column.

        Args:
            column_names: list of strings. Names of columns in the order given by the user.
            Also the user can give the keyword ASC|DESC after the name of each table

        This function works just like SQL's ORDER BY

        e.g.:

        ORDER BY column1 ASC|DESC , column2 ASC|DESC

        so you can have a query like this:

        ORDER BY Country ASC, CustomerName DESC

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

            # the query didnt have ASC|DESC, default is False
            else:
                target_cols_order.append(False)

        # function will sort the given self.data
        self.hyper_sort(self.data,target_cols,0,len(self.data),target_cols_order)

        # return


    def hyper_sort(self,input_list,columns,indexStart,indexEnd,reverses):
        '''
        Arguements:

        -> inpur_list: the list that will be sorted
        -> columns: a list with the indexes of the columns that will be sorted.
        The indexes are given in the order the columns will be sorted (meaning
        in the order they are given in the 'ORDER BY')
        Columns can begiven in any desired order.
        -> indexStart: int - the index of an element of the inpur_list (see Procedure)
        -> indexEnd: int - the index of an element of the inpur_list (see Procedure)
        -> reverses list of booleans - contains the desired orders in which each column
        given in 'columns' will be sorted. True = desc and False = asc

        Returns:
        input_list with elements between the indexes 'indexStart' 'indexEnd' sorted


        Procedure:

        This function first makes a list (input_list_copy) with elements
        of 'input_list' from index 'indexStart' till 'indexEnd' (indexEnd is
        also included, thats why we write 'indexEnd+1').

        This 'sublist' is sorted according to the first column given in the 'columns' list,
        with asc/desc boolean also from the first element of the 'reverses' list.

        After sorting, the function checks the 'sublist' with a for loop if
        there are duplicates in the first column given in the 'columns'.

        Since the table has been sorted, duplicates will be neighbouring. This means that
        in order to find the duplicate elements we do the following:

        -> store the first element ('prev') of the desired column (first column given in the 'columns')
        -> initialize the variable 'initial' with 0 - this variable holds the position in which
        the examined element is first spotted (the examined element is initialy the first
        so thats why it is 0)
        -> start a loop
        -> if the stored element is equal to the current element of the loop,
        check if the current element is the last. If NO, continue.
        If YES, call recursively function 'hyper_sort' to sort according
        to the next column. This is done by passing in the
        arguements columns[1:] and reverses[1:], in order for the function to
        sort on the next column.
        -> if the stored element is NOT equal to the current element of the loop,
        do a recursive call of 'hyper_sort' with indexStart = first and indexEnd = i-1
        and columns[1:], reverses[1:] as before. Then reset 'prev' and 'init' in order
        to have the i-element(current)

        To sum up,

        A sublist is created from indexStart (initial value == 0) till indexEnd (initial value == len(input_list)) and is sorted.
        Then it is checked for duplicates. For each 'block' of duplicates do a recursive
        call to sort it according to the next column

        '''


        if(indexStart == indexEnd):
            return input_list

        input_list_copy = sorted(input_list[indexStart:indexEnd+1],key=lambda x: (x[columns[0]]),reverse=reverses[0])

        #print(input_list_copy)

        prev = input_list_copy[0][columns[0]]
        initial = 0

        for i in range(len(input_list_copy)):

            if input_list_copy[i][columns[0]] == prev:

                if(i== len(input_list_copy)-1):
                    #print(f" found {prev} from {initial} till {i}")

                    # The duplicates are [initial,i]

                    if(len(columns)>1):
                        input_list_copy = self.hyper_sort(input_list_copy,columns[1:],initial,i,reverses[1:])

            else:
                #print(f" found {prev} from {initial} till {i-1}")

                # The duplicates are [initial,i-1]

                if(len(columns)>1):
                    input_list_copy = self.hyper_sort(input_list_copy,columns[1:],initial,i-1,reverses[1:])

                prev = input_list_copy[i][columns[0]]
                initial = i


        input_list[indexStart:indexEnd+1] = input_list_copy


        return input_list


    def group_by(self,groups):
        '''
        Args:
        groups: the string given in the query, containing the GROUP BY list

        Returns:
        a table object with the columns given in the GROUP BY clause (groups)
        with distinct rows
        '''
        
        if(groups is None):
            return
        
        # use the _select_where function to do a 'query': select distinct <groups> from <self>
        s_table = self._select_where(groups, None,None,None,None,None,True)

        return s_table
    

    def distinct(self):
        '''
        Remove duplicate rows in the table

        This function does the following steps:
        ->The function first checks if the PK is in the columns. If so, it immediatly stops,
        since the rows are guaranted to be distinct if the PK column is present

        ->If the PK is not in the columns, the function sorts the entire table by calling the 'order_by' function and giving it
        all the columns. Desc or asc doesnt matter

        ->Then it does a simple loop to remove duplicates (since it is sorted, duplicates will be together)
        '''
        if(self.pk in self.column_names):
            #print("no action required")
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



# The following are the aggregate functions

def getTuple(original,column_names,index):

    tup = ()

    for col in column_names:
        tup = tup + (original.data[index][original.column_names.index(col)],)

    return tup

def max(original,grouped,target_column,column_names):
    '''
    Arguements:

    -> original: this is a reference to the original table, meaning the table returnedd by FROM .. WHERE -
    containing all the columns and rows.
    -> grouped: this is the table returned from GROUP BY and modified by the aggregate functions.
    It contains the columns of GROUP BY clause as well as extra columns from aggregate functions
    that have already been applied
    -> target_column: this is the string inside the sql statement of the aggregate function, it
    basically contains the name of the target column and/or the 'distinct' statement.
    -> column_names: this is a list containing all the column names of the columns that make up
    the groups, meaning the columns in GROUP BY clause.


    Returns:

    A new table that consists of all the data of table grouped and the appended column of the aggregate function.


    Procedure:

    
    
    '''

    # check for 'distinct'



    target = original.column_names.index(target_column)

    orders = column_names.copy()



    if(target_column not in grouped.column_names):

        orders.append(target_column+" desc")

    original.order_by(orders)

    prev = original.data[0]

    prev = getTuple(original,column_names,0)


    max = []
    max.append(original.data[0][target])


    for i in range(len(original.data)):

        if(prev != getTuple(original,column_names,i)):

            max.append(original.data[i][target])

            prev = getTuple(original,column_names,i)


        elif(prev == getTuple(original,column_names,i) and i == len(original.data)):
            max.append(original.data[i][target])
            print("found group",prev)


    c_names = grouped.column_names
    c_names.append("agg_max_" +target_column)
    c_types = grouped.column_types
    c_types.append(original.column_types[target])
    pk = grouped.column_names[0]
    n_table = Table("temp", c_names, c_types, pk)
    n_table.data = []

    for i in range(len(grouped.data)):

        (grouped.data[i]).append(max[i])
        n_table.data.append(grouped.data[i])


    return n_table


def min(original,grouped,target_column,column_names):
    '''
    Arguements:

    -> original: this is a reference to the original table, meaning the table returnedd by FROM .. WHERE -
    containing all the columns and rows.
    -> grouped: this is the table returned from GROUP BY and modified by the aggregate functions.
    It contains the columns of GROUP BY clause as well as extra columns from aggregate functions
    that have already been applied
    -> target_column: this is the string inside the sql statement of the aggregate function, it
    basically contains the name of the target column and/or the 'distinct' statement.
    -> column_names: this is a list containing all the column names of the columns that make up
    the groups, meaning the columns in GROUP BY clause.


    Returns:

    A new table that consists of all the data of table grouped and the appended column of the aggregate function.


    Procedure:

    
    
    '''

    # check for 'distinct'
    input_target = target_column.split(' ')
    input_target_column = input_target[0]

    if(input_target[0]=='distinct'):
        input_target_column = input_target[1]

    target = original.column_names.index(input_target_column)

    # orders: list of the column names that will be sorted
    orders = column_names.copy()

    # if the target column is not
    if(input_target_column not in grouped.column_names):
        orders.append(input_target_column)

    # sort the table object by using order_by function
    original.order_by(orders)

    prev = original.data[0]

    prev = getTuple(original,column_names,0)

    min = []
    min.append(original.data[0][target])


    for i in range(len(original.data)):

        if(prev != getTuple(original,column_names,i)):

            min.append(original.data[i][target])

            prev = getTuple(original,column_names,i)


        elif(prev == getTuple(original,column_names,i) and i == len(original.data)):
            min.append(original.data[i][target])


    # create table object with the data and columns of grouped table object
    # as well as a new column with the min values
    c_names = grouped.column_names
    c_names.append("agg_min_"+  target_column.replace(' ', '_'))
    c_types = grouped.column_types
    c_types.append(original.column_types[target])
    pk = grouped.column_names[0]
    n_table = Table("temp", c_names, c_types, pk)
    n_table.data = []

    # append the min values to a new column
    for i in range(len(grouped.data)):
        (grouped.data[i]).append(min[i])
        n_table.data.append(grouped.data[i])


    return n_table



def sum(original, grouped, target_column, column_names):
    '''
    Arguements:

    -> original: this is a reference to the original table - containing all the columns and rows.
    -> grouped: this is the table returned from group by and modified by the aggregate functions.
    -> target_column: this is the string inside the sql statement of the aggregate function, it
    basically contains the name of the target column and/or the 'distinct' statement.
    -> column_names: this is a list containing all the column names of the columns that make up
    the groups created due to group by.


    Returns:

    A new table that consists of all the data of table grouped and the appended column of the aggregate function.


    Raises Exception:

    Custom exception raised if the target_column is not numeric.


    Procedure:

    The function creates a variable target containing the index of target_column in the original table.
    It also creates a list of the indexes of the column_names in the original table.
    Then, it orders the reference of the original table by the group columns (column_names) and
    the target column - this helps the function distinguish between groups as well as deal with the distinct case.

    Afterwards, it creates a list of sums, which is going to contain the summed values of each group.
    At this point, the function starts a loop, searching through every single row of the original table, while also
    remembering the previous iteration's row in a variable 'prev'.
    On each iteration: the loop checks whether the previous row is of the same group as the current row by checking for equality
    between the group-by column names (the indexes of which, are stored in the list 'groups') and, if so, adds
    the target-th value of the row to the sums list.

    If the functions was asked to sum only distinct values, then on each iteration: the function also checks wether the target-th
    vaule is equal between the two rows, and only adds to sums if the two values are unequal.

    To finish off, the function creates a new table, containing all the rows (and their data) of table grouped, plus a new
    row of the appropriate aggregate function name containing the sums of each group.


    To sum up:

    The function sums up the data of the target_column for each group in column_names 
    (if distinct was specidied, it ignores duplicate values while iterating through the data).
    '''

    #we check wether 'distinct' was specified.
    distinct = False
    input_target = target_column.split(' ')
    input_target_column = input_target[0]

    if(input_target[0]=='distinct'):
        distinct = True
        input_target_column = input_target[1]


    target = original.column_names.index(input_target_column)
    groups = [original.column_names.index(elem) for elem in column_names]
    if not(str(original.column_types[target]) == "<class 'int'>"):
            raise Exception("The aggregate functions sum, avg are valid on numeric columns only!")

    #we sort the original table.
    orders = column_names.copy()
    if(input_target_column not in grouped.column_names):
        orders.append(input_target_column)
    original.order_by(orders)


    sums = [None] * len(grouped.data)
    interval = 0

    if(distinct):

        prev = original.data[0]

        for elem in list(original.data[1:]):
            if(sums[interval] is None):
                sums[interval] = prev[target]

            #we must check whether the two rows are of the same group.
            #to achieve this, we add the values of each column that takes part in the group
            #in a list for both the current, and the previous row.
            tlist1 = [prev[groups[i]] for i in range(len(groups))]
            tlist2 = [elem[groups[i]] for i in range(len(groups))]

            if(tlist1 == tlist2 and elem[target] != prev[target]):
                sums[interval] += elem[target]
            elif(tlist1 == tlist2 and elem[target] == prev[target]):
                pass
            else:
                interval += 1
            prev = elem

        #if the last row is its own group, the above loop does not update the sums list
        #and leaves it empty. Thus, in such case, we update it manually.
        if(sums[-1] is None):
            sums[-1] = prev[target]
    else:

        prev = original.data[0]

        for elem in list(original.data[1:]):
            if(sums[interval] is None):
                sums[interval] = prev[target]

            tlist1 = [prev[groups[i]] for i in range(len(groups))]
            tlist2 = [elem[groups[i]] for i in range(len(groups))]
            if(tlist1 == tlist2):
                sums[interval] += elem[target]
            else:
                interval += 1
            prev = elem
        if(sums[-1] is None):
            sums[-1] = prev[target]

    c_names = grouped.column_names
    c_names.append("agg_sum_" + target_column.replace(' ', '_'))
    c_types = grouped.column_types
    c_types.append(original.column_types[target])
    pk = grouped.column_names[0]
    n_table = Table("temp", c_names, c_types, pk)
    n_table.data = []

    interval = 0
    for d in grouped.data:
        d.append(sums[interval])
        n_table.data.append(d)
        interval += 1

    return n_table

def count(original, grouped, target_column, column_names):
    '''
    Arguements:

    -> original: this is a reference to the original table - containing all the columns and rows.
    -> grouped: this is the table returned from group by and modified by the aggregate functions.
    -> target_column: this is the string inside the sql statement of the aggregate function, it
    basically contains the name of the target column and/or the 'distinct' statement.
    -> column_names: this is a list containing all the column names of the columns that make up
    the groups created due to group by.


    Returns:

    A new table that consists of all the data of table grouped and the appended column of the aggregate function.


    Procedure:

    The function creates a variable target containing the index of target_column in the original table.
    It also creates a list of the indexes of the column_names in the original table.
    Then, it orders the reference of the original table by the group columns (column_names) and
    the target column - this helps the function distinguish between groups as well as deal with the distinct case.

    Afterwards, it creates a list of counts, which will contain the counted data of each group.
    At this point, the function starts a loop, searching through every single row of the original table, while also
    remembering the previous iteration's row in a variable 'prev'.
    On each iteration: the loop checks whether the previous row is of the same group as the current row by checking for equality
    between the group-by column names (the indexes of which, are stored in the list 'groups') and, if so, counts up by 1 the number
    of rows assiciated with said group and target column (in the list of counts).

    If the functions was asked to count only distinct values, then on each iteration: the function also checks wether the target-th
    vaule is equal between the two rows, and only adds to counts if the two values are unequal.

    To finish off, the function creates a new table, containing all the rows (and their data) of table grouped, plus a new
    row of the appropriate aggregate function name containing the counts of each group.


    To sum up:

    The function counts how many rows are assiciated with each group (in column_names) by their values in the target_column.
    (if distinct was specidied, it ignores duplicate values while iterating through the data).
    '''

    #we check whether 'distinct' was specified.
    distinct = False
    input_target = target_column.split(' ')
    input_target_column = input_target[0]

    if(input_target[0]=='distinct'):
        distinct = True
        input_target_column = input_target[1]


    target = original.column_names.index(input_target_column)
    groups = [original.column_names.index(elem) for elem in column_names]

    #we sort the original table.
    orders = column_names.copy()
    if(input_target_column not in grouped.column_names):
        orders.append(input_target_column)
    original.order_by(orders)


    counts = [1] * len(grouped.data)
    interval = 0

    if(distinct):

        prev = original.data[0]

        for elem in list(original.data[1:]):

            #we must check whether the two rows are of the same group.
            #to achieve this, we add the values of each column that takes part in the group
            #in a list for both the current, and the previous row.
            tlist1 = [prev[groups[i]] for i in range(len(groups))]
            tlist2 = [elem[groups[i]] for i in range(len(groups))]

            if(tlist1 == tlist2 and elem[target] != prev[target]):
                counts[interval] += 1
            elif(tlist1 == tlist2 and elem[target] == prev[target]):
                pass
            else:
                interval += 1
            prev = elem
    else:

        prev = original.data[0]

        for elem in list(original.data[1:]):
            tlist1 = [prev[groups[i]] for i in range(len(groups))]
            tlist2 = [elem[groups[i]] for i in range(len(groups))]

            if(tlist1 == tlist2):
                counts[interval] += 1
            else:
                interval += 1
            prev = elem

    c_names = grouped.column_names
    c_names.append("agg_count_" + target_column.replace(' ', '_'))
    c_types = grouped.column_types
    c_types.append(original.column_types[target])
    pk = grouped.column_names[0]
    n_table = Table("temp", c_names, c_types, pk)
    n_table.data = []

    interval = 0
    for d in grouped.data:
        d.append(counts[interval])
        n_table.data.append(d)
        interval += 1

    return n_table


def avg(original, grouped, target_column, column_names):
    '''
    Arguements:

    -> original: this is a reference to the original table - containing all the columns and rows.
    -> grouped: this is the table returned from group by and modified by the aggregate functions.
    -> target_column: this is the string inside the sql statement of the aggregate function, it
    basically contains the name of the target column and/or the 'distinct' statement.
    -> column_names: this is a list containing all the column names of the columns that make up
    the groups created due to group by.


    Returns:

    A new table that consists of all the data of table grouped and the appended column of the aggregate function.
    

    Raises Exception:

    Custom exception raised if the target_column is not numeric.


    Procedure:

    The function creates a variable target containing the index of target_column in the original table.
    It also creates a list of the indexes of the column_names in the original table.
    Then, it orders the reference of the original table by the group columns (column_names) and
    the target column - this helps the function distinguish between groups as well as deal with the distinct case.

    Afterwards, it creates a list of counts, which will contain the counted data of each group and also
    a list of sums, which is going to contain the summed values of each group.
    At this point, the function starts a loop, searching through every single row of the original table, while also
    remembering the previous iteration's row in a variable 'prev'.
    On each iteration: the loop checks whether the previous row is of the same group as the current row by checking for equality
    between the group-by column names (the indexes of which, are stored in the list 'groups'). In that case, it counts up by 1 the number
    of rows assiciated with said group and target column (in the list of counts) and also adds the target-th value of the row to the sums list.

    If the functions was asked to count only distinct values, then on each iteration: the function also checks wether the target-th
    vaule is equal between the two rows, and only adds to counts and sums if the two values are unequal.

    To finish off, the function creates a new table, containing all the rows (and their data) of table grouped, plus a new
    row of the appropriate aggregate function name containing the average value (sums/counts) of each group.


    To sum up:

    The function counts how many rows are assiciated with each group (in column_names) by their values in the target_column.
    Also it sums up the data of the target_column for each group in column_names.
    Finally, it takes the average value of the target_column data for each group by dividing the above.
    (if distinct was specidied, it ignores duplicate values while iterating through the data).
    '''

    #we check whether 'distinct' was specified.
    distinct = False
    input_target = target_column.split(' ')
    input_target_column = input_target[0]

    if(input_target[0]=='distinct'):
        distinct = True
        input_target_column = input_target[1]


    target = original.column_names.index(input_target_column)
    groups = [original.column_names.index(elem) for elem in column_names]

    if not(str(original.column_types[target]) == "<class 'int'>"):
            raise Exception("The aggregate functions sum, avg are valid on numeric columns only!")

    #we sort the original table.
    orders = column_names.copy()
    if(input_target_column not in grouped.column_names):
        orders.append(input_target_column)
    original.order_by(orders)

    sums = [None] * len(grouped.data)
    counts = [1] * len(grouped.data)
    interval = 0

    if(distinct):

        prev = original.data[0]

        for elem in list(original.data[1:]):
            if(sums[interval] is None):
                sums[interval] = prev[target]

            #we must check whether the two rows are of the same group.
            #to achieve this, we add the values of each column that takes part in the group
            #in a list for both the current, and the previous row.
            tlist1 = [prev[groups[i]] for i in range(len(groups))]
            tlist2 = [elem[groups[i]] for i in range(len(groups))]

            if(tlist1 == tlist2 and elem[target] != prev[target]):
                sums[interval] += elem[target]
                counts[interval] += 1
            elif(tlist1 == tlist2 and elem[target] == prev[target]):
                pass
            else:
                interval += 1
            prev = elem
        
        #if the last row is its own group, the above loop does not update the sums list
        #and leaves it empty. Thus, in such case, we update it manually.
        if(sums[-1] is None):
            sums[-1] = prev[target]
    else:

        prev = original.data[0]
        interval = 0
        for elem in list(original.data[1:]):
            if(sums[interval] is None):
                sums[interval] = prev[target]

            tlist1 = [prev[groups[i]] for i in range(len(groups))]
            tlist2 = [elem[groups[i]] for i in range(len(groups))]
            if(tlist1 == tlist2):
                sums[interval] += elem[target]
                counts[interval] += 1
            else:
                interval += 1
            prev = elem

        if(sums[-1] is None):
            sums[-1] = prev[target]

    c_names = grouped.column_names
    c_names.append("agg_avg_" + target_column.replace(' ', '_'))
    c_types = grouped.column_types
    c_types.append(original.column_types[target])
    pk = grouped.column_names[0]
    n_table = Table("temp", c_names, c_types, pk)
    n_table.data = []

    interval = 0
    for d in grouped.data:
        d.append(sums[interval]/counts[interval])
        n_table.data.append(d)
        interval += 1

    return n_table
