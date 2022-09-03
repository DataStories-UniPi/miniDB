from __future__ import annotations
import shutil
from tabulate import tabulate
import pickle
import os
from miniDB.btree import Btree
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


    def _select_where(self, return_columns, condition=None, order_by=None, desc=True, top_k=None):
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
        '''


        # Select DISTINCT
        distinct = False
        # check if distinct is in return_columns (it means is after select)
        if 'distinct' in return_columns:
            distinct = True
            # take columns (remove distinct from return columns)
            return_columns_tmp = return_columns.split('distinct')[1].strip().split(',')
            # select everything
            if '*' in return_columns_tmp:
                return_cols = [i for i in range(len(self.column_names))]
            # select specified columns
            else:
                return_cols = [self.column_names.index(col.strip()) for col in return_columns_tmp]

        # Basic select
        else:
            # return_cols = [self.column_names.index(col.strip()) for col in return_columns.split(',')]

            # if * return all columns, else find the column indexes for the columns specified
            if return_columns == '*':
                return_cols = [i for i in range(len(self.column_names))]
            else:
                return_cols = [self.column_names.index(col.strip()) for col in return_columns.split(',')]


        # if condition is None, return all rows
        # if not, return the rows with values where condition is met for value
        if condition is not None:
            tmp_condition = condition.split(" ")[1]
            rows = []
            # select between
            if 'between' in tmp_condition:
                column_name = condition.split('between')[0].strip()
                values = condition.split('between')[1].split('and')
                column = self.column_by_name(column_name)
                try:
                    # Take only data between a and b
                    for ind, x in enumerate(column):
                        if int(values[0]) <= x <= int(values[1]):
                            rows.append(ind)
                except:
                    pass
            # select in
            elif 'in' in tmp_condition:
                column_name = condition.split('in')[0].strip()
                values = condition.split('(')[1].split(')')[0].split(',')
                column = self.column_by_name(column_name)
                # Take only data much in parentheses values
                for ind, x in enumerate(column):
                    if str(x) in str(values):
                        rows.append(ind)

            # select like
            elif 'like' in tmp_condition:
                column_name, value = condition.split('like')[0].strip(), condition.split('like')[1].strip().replace("'", "")
                column = self.column_by_name(column_name)

                if '%' in value:
                    like = [i for i, j in enumerate(value) if j == '%']
                    # Start end
                    if len(like) == 2 and like[0] == 0 and like[1] + 1 == len(value):
                        value = value.split("%")[1]
                        # Take only data much include value between %'s
                        for ind, x in enumerate(column):
                            if value in str(x):
                                rows.append(ind)

                    # Start
                    elif like[0] == 0:
                        value = value.split("%")[1]
                        # Take only data ends with value after %
                        for ind, x in enumerate(column):
                            if value == str(x)[-len(value):]:
                                rows.append(ind)

                    # End
                    elif like[0] + 1 == len(value):
                        value = value.split("%")[0]
                        # Take only data starts with value before %
                        for ind, x in enumerate(column):
                            if value == str(x)[:len(value)].strip():
                                rows.append(ind)

                    # Middle
                    elif 0 < value.index('%') < len(value):
                        values = value.split("%")
                        # Take only data start and end with value before and after %
                        for ind, x in enumerate(column):
                            if values[1] == str(x)[-len(values[1]):] and values[0] == str(x)[:len(values[0])+1].strip():

                                rows.append(ind)
            # continue normally...
            else:
                column_name, operator, value = self._parse_condition(condition)
                column = self.column_by_name(column_name)

                rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
        else:
            rows = [i for i in range(len(self.data))]

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

        # Select DISTINCT
        if distinct:
            tmp_list = []
            tmp_tbl = s_table
            index = 0
            # for table's data check for multiplied data and ignore them using tmp list and pop method
            for i in range(len(s_table.data)):
                if s_table.data[index] not in tmp_list:
                    tmp_list.append(s_table.data[index])
                    index += 1
                else:
                    tmp_tbl.data.pop(index)  # remove item from objects data

            s_table = tmp_tbl

        # Basic select
        else:
            s_table.data = s_table.data[:int(top_k)] if isinstance(top_k, str) else s_table.data

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


    def _inl_join(self, table_left:Table , table_right : Table , condition):
        # columns and operator 

        '''
        query should bt like selec * from table1 inlj table 2  on table1.col1 = table2.col2
        '''

        if table_left.pk is None  and table_right.pk is not None :
            # We should create index for the right table 
            index = Btree(3)
            for right_indx, key in enumerate(table_right.column_by_name(table_right.pk)):
                index.insert(key,right_indx)
        elif table_left.pk is not None and table_right.pk is None:
            # if the left table has the pk we must reverse them and use the one with the primary key 
            temp =table_right
            table_right = table_left
            table_left = temp
            # we should create index for the right table 
            index = Btree(3)
            for right_indx ,key in enumerate(table_right.column_by_name(table_right.pk)):
                index.insert(key,right_indx)

            # Getting columns and operator 

            column_name_left , operator , column_name_right = table_left._parse_condition(condition, join=True)
            if operator != '=':
                print ("You should only use  '=' operator !!!")
            try :
                column_index_left = table_left.column_names.index(column_name_left)
            except:
                raise Exception (f'Column "{column_name_left}" does not exist in left table. Valid columns: {table_left.column_names}.')
            try :
                column_index_right = table_right.column_names.index(column_name_right)
            except:
                raise Exception (f'Column "{column_name_right}" dont exist in right table. Valid columns: {table_right.column_names}.')
            # get the column names of both tables with the table name in front
            # ex. for left -> name becomes left_table_name_name etc
            left_names = [f'{table_left._name}.{colname}' if table_left._name!='' else colname for colname in table_left.column_names]
            right_names = [f'{table_right._name}.{colname}' if table_right._name!='' else colname for colname in table_right.column_names]

            # define the new tables name, its column names and types
            join_table_name = ''
            join_table_colnames = left_names+right_names
            join_table_coltypes = table_left.column_types+table_right.column_types
            join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)


            # index nested loop join  for loop
            for row_left in table_left.data:
                left_value = row_left[column_index_left]
                results = index.find(operator,left_value)
                if(len(results)>0):
                    for i in results:
                        if get_op(operator, left_value, table_right.data[i][column_index_right]):
                            join_table._insert(row_left + table_right.data[i])

            return join_table



    def _sm_join(self,table_left: Table, table_right: Table, condition):
        # Create directory 'tempFiles' for temporary files
        os.mkdir("tempFiles")

        # two dictionaries to store table data and keys
        dict_left = {}
        dict_right = {}
        # get columns and operator
        column_name_left, operator, column_name_right = table_left._parse_condition(condition, join=True)
        if operator != '=':
            print("Wrong operator!!! You can only use the '=' operator")
            return
        # try to find both columns, if you fail raise error
        try:
            column_index_left = [table_left.column_names.index(column_name_left)]
        except:
            raise Exception(f'Column "{column_name_left}" dont exist in left table. Valid columns: {table_left.column_names}.')
        try:
            column_index_right = [table_right.column_names.index(column_name_right)]
        except:
            raise Exception(f'Column "{column_name_right}" dont exist in right table. Valid columns: {table_right.column_names}.')

        # select all rows
        left_table_rows = [i for i in range(len(table_left.data))]
        right_table_rows = [i for i in range(len(table_right.data))]

        # save in leftTableFile all the values of the selected column in left table
        with open("tempFiles/leftTableFile",'w+') as left_table_file:
            for i in left_table_rows:
                for j in column_index_left:
                    left_table_file.write(str(table_left.data[i][j])+'\n')
                    dict_left[table_left.data[i][j]] = table_left.data[i][:]
        # save in leftTableFile all the values of the selected column in left table
        with open("tempFiles/rightTableFile",'w+') as right_table_file:
            for i in right_table_rows:
                for j in column_index_right:
                    right_table_file.write(str(table_right.data[i][j])+'\n')
                    dict_right[table_right.data[i][j]] = table_right.data[i][:]

        # For left table - dictionary create a new sorted dictionary
        table_l = Table.ExternalMergeSort()
        keys_left = table_l.ext_merge_sort('leftTableFile')
        new_dict_left = {}
        for i in keys_left:
            line = i.rstrip("\n")
            new_dict_left[i] = dict_left[line]

        # For right table - dictionary create a new sorted dictionary
        table_r = Table.ExternalMergeSort()
        keys_right = table_r.ext_merge_sort('rightTableFile')
        new_dict_right = {}
        for i in keys_right:
            line = i.rstrip("\n")
            new_dict_right[i] = dict_right[line]


        left_table_file.close()
        right_table_file.close()

        # get the column names of both tables with the table name in front
        # ex. for left -> name becomes left_table_name_name etc
        left_names = [f'{self._name}.{colname}' if self._name!='' else colname for colname in self.column_names]
        right_names = [f'{table_right._name}.{colname}' if table_right._name!='' else colname for colname in table_right.column_names]

        # define the new tables name, its column names and types
        join_table_name = ''
        join_table_colnames = left_names+right_names
        join_table_coltypes = self.column_types+table_right.column_types
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)

        # Joins the two tables
        for line_left in new_dict_left.values():
            left_value = line_left[column_index_left[0]]
            for line_right in new_dict_right.values():
                right_value = line_right[column_index_right[0]]
                if get_op(operator, left_value, right_value):
                    join_table._insert(line_left+line_right)

        # Delete "tempFiles" folder
        shutil.rmtree("tempFiles")

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




#Class for External Merge Sort    
    class ExternalMergeSort:

        # the number of total temp files
        number_of_files = 1


        def ext_merge_sort(self,file):
            final_list = []
            self.chunk_file(file)
            with open('tempFiles/1','r') as last_table_read:
                for line in last_table_read:
                    final_list.append(line)
            last_table_read.close() 
            return final_list

        # Chunks original file into smaller pieces. Each piece has 3 values
        def chunk_file(self,original_file):
            flag = True
            # counter for lines
            current_index = 0 
            # the number of total temp files
            number_of_files = 1
            with open('tempFiles/'+str(original_file), 'r') as read_obj:
                chunk_values = []
                # Line by line copy data from original file to dummy file
                for line in read_obj:
                    # If current line number matches the given line number then skip copying
                    if current_index <= 2:
                        chunk_values.append(line)
                        # if we have 3 lines we sort them and write them in files
                        if current_index == 2:
                            chunk_values.sort()
                            # write the 3 lines in a new temp file
                            with open('tempFiles/'+str(number_of_files), 'w+') as write_obj:
                                for i in range(0,len(chunk_values)):
                                    if chunk_values[i]!="\n":
                                        write_obj.write(str(chunk_values[i]))
                                number_of_files+=1
                            write_obj.close()
                            current_index = 0
                            chunk_values = []
                        else:    
                            current_index += 1
                # if there are remaining lines when the loop stops, sort them and save the in another temp file
                if len(chunk_values) != 0:
                    flag=False;
                    chunk_values.sort()
                    with open('tempFiles/'+str(number_of_files), 'w+') as write_obj:
                        for i in range(0,len(chunk_values)):
                            if chunk_values[i]!="\n":
                                write_obj.write(str(chunk_values[i]))
                    write_obj.close()
                # number_of_files is increased for the next file to be created. If there is no next file we restore number_of_files value
                elif flag:
                    number_of_files-=1
            self.mergeSortedFiles(number_of_files)
            return

        def mergeSortedFiles(self,number_of_files):
            # merged files counter
            file_counter = 1
            # original file counter
            left_counter = 1

            left_list=[]
            right_list=[]
            # this will store left+right lists
            combined_list=[]

            for i in range(0,int(number_of_files/2)):
                left_list=[]
                right_list=[]
                combined_list=[]

                # reads chunked files and appends them in left and right lists
                with open('tempFiles/'+str(left_counter),'r') as left_read_obj, open('tempFiles/'+str(left_counter+1),'r') as right_read_obj:
                    for line1 in left_read_obj:
                        left_list.append(line1)
                    for line2 in right_read_obj:
                        right_list.append(line2)
                left_read_obj.close()
                right_read_obj.close()

                # the combined list of left and right list is sorted with merge-sort algorithm
                combined_list = self.mergeSortAlgorithm(left_list,right_list)

                # write the combined list into a temp file
                with open('tempFiles/'+str(file_counter), 'w+') as write_obj:
                    for i in range(0,len(combined_list)):
                        write_obj.write(str(combined_list[i]))
                write_obj.close()
                left_counter+=2 
                file_counter+=1 


            # if number_of_files is ODD we have one file remaining so we combine it with the last merged file
            if number_of_files%2!=0:
                left_list=[]
                right_list=[]
                combined_list=[]

                # we open the last merged file and the file remaining
                with open('tempFiles/'+str(file_counter-1), 'r') as left_read_obj, open('tempFiles/'+str(left_counter),'r') as right_read_obj:
                    for line1 in left_read_obj:
                        left_list.append(line1)
                    for line2 in right_read_obj:
                        right_list.append(line2)
                left_read_obj.close()
                right_read_obj.close()

                # the combined list of left and right list is sorted with merge-sort algorithm
                combined_list = self.mergeSortAlgorithm(left_list,right_list)

                # write the combined list into a temp file
                with open('tempFiles/'+str(file_counter-1), 'w+') as write_obj:
                    for i in range(0,len(combined_list)):
                        write_obj.write(str(combined_list[i]))
                write_obj.close()

            ''' 
                file_counter is always number_of_files + 1. If file_counter is at least 3 then we have at least 2 files (number_of_files >= 2) so we need 
                to call again mergeSortedFiles function 
            '''
            if file_counter>=3:
                number_of_files = file_counter-1 
            else:
                number_of_files=1

            # if we have more than one files we enter mergeSortedFiles function again until we have only 1 file
            if number_of_files!=1:
                self.mergeSortedFiles(number_of_files)

            return


        # this is just merge-sort algorithm
        # Remember: we have already chunked our file so we use 2 different chunks for the algorithm
        def mergeSortAlgorithm(self,left,right):
            myList = []
            if len(left) >= 1 and len(right) >= 1:
                # Two iterators for traversing the two halves
                i = 0
                j = 0

                while i < len(left) and j < len(right):
                    if left[i] <= right[j]:
                      # The value from the left half has been used
                      myList.append(left[i])
                      # Move the iterator forward
                      i += 1
                    else:
                        myList.append(right[j])
                        j += 1


                # For all the remaining values
                while i < len(left):
                    myList.append(left[i])
                    i += 1


                while j < len(right):
                    myList.append(right[j])
                    j += 1

            return myList
