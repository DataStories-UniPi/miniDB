from __future__ import annotations
from tabulate import tabulate
import pickle
import os
import math
from btree import Btree
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

    def __init__(self, name=None, column_names=None, column_types=None, column_extras = None, primary_key=None, load=None):
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
            for val in column_extras: #we check to see what args came for the split
                if not (val == 'unique' or val == 'not null') and val != 'None' and val != '':
                    raise ValueError(f'{val}\' is not a valid keyword!')
            print(column_extras) #if we remove this the whole project doent work
            self.column_extras = column_extras #column extras has to be initialised
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
            # try:
            if row[i] != 'null': # case sensitive value,not actually NULL
                row[i] = self.column_types[i](row[i])
            # except:
            #     raise ValueError(f'ERROR -> Value {row[i]} of type {type(row[i])} is not of type {self.column_types[i]}.')

            # if value is to be appended to the primary_key column, check that it doesnt alrady exist (no duplicate primary keys)
            if i == self.pk_idx and row[i] in self.column_by_name(self.pk):
                raise ValueError(f'## ERROR -> Value {row[i]} already exists in primary key column.')
            if len(self.column_extras) == len(row):   #for not nul
                if self.column_extras[i] == 'not null' and row[i] == 'null':
                    raise ValueError(f'Value {row[i]} can not be NULL.')
                if self.column_extras[i] == 'unique': # Search unique using btree
                    if len(self.column_by_name(self.column_names[i]))>0:
                        btree = Btree(2)
                        for btree_ptr_index, record_value in enumerate(self.column_by_name(self.column_names[i])):
                            btree.insert(str(record_value),btree_ptr_index)
                        if btree.find("=",row[i]) != []:
                            raise ValueError(f'This Value {row[i]} already exists.')

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

        # if * return all columns, else find the column indexes for the columns specified
        if return_columns == '*':
            return_cols = [i for i in range(len(self.column_names))]
        else:
            return_cols = [self.column_names.index(col.strip()) for col in return_columns.split(',')]

        # if condition is None, return all rows
        # if not, return the rows with values where condition is met for value
        if condition is not None:
            column_name, operator, value = self._parse_condition(condition)
            column = self.column_by_name(column_name)
            rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
        else:
            rows = [i for i in range(len(self.data))]

        # top k rows
        # rows = rows[:int(top_k)] if isinstance(top_k,str) else rows
        # copy the old dict, but only the rows and columns of data with index in rows/columns (the indexes that we want returned)
        dict = {(key): ([[self.data[i][j] for j in return_cols] for i in rows] if key == "data" else value) for
                key, value in self.__dict__.items()}

        # we need to set the new column names/types and no of columns, since we might
        # only return some columns
        dict['column_names'] = [self.column_names[i] for i in return_cols]
        dict['column_types'] = [self.column_types[i] for i in return_cols]

        s_table = Table(load=dict)
        if order_by:
            s_table.order_by(order_by, desc)

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
            opsseq += 1
            if get_op(operator, x, value):
                rows1.append(ind)

        # btree find
        rows = bt.find(operator, value)

        # same as simple select from now on
        rows = rows[:top_k]
        # TODO: this needs to be dumbed down
        dict = {(key): ([[self.data[i][j] for j in return_cols] for i in rows] if key == "data" else value) for
                key, value in self.__dict__.items()}

        dict['column_names'] = [self.column_names[i] for i in return_cols]
        dict['column_types'] = [self.column_types[i] for i in return_cols]

        s_table = Table(load=dict)
        if order_by:
            s_table.order_by(order_by, desc)

        s_table.data = s_table.data[:int(top_k)] if isinstance(top_k, str) else s_table.data

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
        join_table_colextras = self.column_extras + table_right.column_extras
        join_table_colnames = left_names + right_names
        join_table_coltypes = self.column_types + table_right.column_types
        # new version of the updated table
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes, column_extras=join_table_colextras)
        #the updated table has more arguments


        # count the number of operations (<,> etc)
        no_of_ops = 0
        # this code is dumb on purpose... it needs to illustrate the underline technique
        # for each value in left column and right column, if condition, append the corresponding row to the new table
        for row_left in self.data:
            left_value = row_left[column_index_left]
            for row_right in table_right.data:
                right_value = row_right[column_index_right]
                no_of_ops += 1
                if get_op(operator, left_value, right_value):  # EQ_OP
                    join_table._insert(row_left + row_right)

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
            for i in range(0,len(self.column_extras)):
                if self.column_extras[i] == 'unique':
                     headers[i] = headers[i] + ' #UN#'
                if self.column_extras[i] == 'not null':
                     headers[i] = headers[i] + ' #NN#'

        # detect the rows that are not full of nones (these rows have been deleted)
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

        # cast the value with the specified column's type and return the column name, the operator and the casted value
        left, op, right = split_condition(condition)
        if left not in self.column_names:
            raise ValueError(f'Condition is not valid (cant find column name)')
        coltype = self.column_types[self.column_names.index(left)]
        if right == 'null':  # ignore null
            return left, op, right
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

    def _inlj_join(self, right_table: Table, condition):
        '''
        Join table (left) with a table on the right where a condition is met using index nested loop join.
        Args:
            the condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]btree_ptr_index' or
                'btree_ptr_index[<,<=,==,>=,>]column'.

                Operators supported: (<,<=,==,>=,>)

                 queries to run:
                 select * from department join instructor on dept_name=dept_name   failed message
                 select * from instructor join department on dept_name=dept_name
                 select * from instructor inlj join course on dept_name=dept_name
                 select * from prereq inlj join course on course_id=course_id   pk on the second table
                 select * from prereq inlj join course on course_id>course_id  could be inlj if =

                 get columns and operator
        '''
        column_name_left, operator, column_name_right = self._parse_condition(condition, join=True)
        # Inner nested loop join requires the join to be equi-join or natural join and the embedded table
        # must be supported by an index
        if operator != "=":
            print("can't join tables using inlj, attempting inner join instead.")
            join_table = self._inner_join(right_table, condition)  # inner join parameters
            return join_table
        # Checking if the table can be indexed
        if right_table.pk is None:
            if self.pk is None:
                # If no indexes, simply run inner join
                print("Missing indexes for tables,inlj cant run, trying inner join instead.")
                join_table = self._inner_join(right_table, condition)  # inner join parameters
            else:
                # If left can be indexed, but right can't, switch places, for a more otpimal way to join
                # the result is the same, and there is no need to run inlj where the inner table is not indexed
                join_table = right_table._inl_join(self, condition)  # inner join parameters
            return join_table
        # Finding the height of the Btree
        record_count = right_table.column_by_name(right_table.pk)  #
        height = round(math.log(len(record_count)))
        # Create Btree index
        btree_index = Btree(height)
        # inserting the value of each primary key's record and the index to the appropriate node
        for btree_ptr_index, record_value in enumerate(right_table.column_by_name(right_table.pk)):
            btree_index.insert(record_value, btree_ptr_index)
        # try to see if there are any indexes for pending columns to join using inlj, if not error
        # and show the available ones
        try:
            left_column_index = self.column_names.index(column_name_left)
        except:
            raise Exception(
                f'Index for column "{column_name_left}" doesnt exist in left table. Valid columns: {self.column_names}.')
        try:
            right_column_index = right_table.column_names.index(column_name_right)
        except:
            raise Exception(
                f'Index for column "{column_name_right}" doesnt exist in right table. Valid columns: {right_table.column_names}.')

        # get the column names so that they later match the condition
        # the left column name becomes left_table_name_name
        left_names = [f'{self._name}.{colname}'
                      if self._name != ''  # if the table has a name
                      else
                      colname for colname in self.column_names  # or it searches for a name in the columns
                      ]
        # likewise
        right_names = [f'{right_table._name}.{colname}' if right_table._name != '' else colname for colname in
                       right_table.column_names]

        # creating a new temporary table with its values and types
        join_table_name = ''
        join_table_colextras = self.column_extras + right_table.column_extras
        join_table_colnames = left_names + right_names
        join_table_coltypes = self.column_types + right_table.column_types
        #new version of the updated table
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes, column_extras=join_table_colextras)

        # INLJ with a cost of blocks in the left_table + left_record records * the records in the right's table index
        for outer_record in self.data:  # going through the first table(with the fewer records)
            left_record = outer_record[left_column_index]
            matching_index = btree_index.find(operator, left_record)  # condition
            if len(matching_index) > 0:  # if there are records in the index that satisfy the condition
                for match_id in matching_index:  # however many are the records in the nested table
                    join_table._insert(outer_record + right_table.data[match_id])  # putting the results in a table

        return join_table




    def _smj_join(self, table_right: Table, condition):
        '''
        Join table (left) with a supplied table (right) where condition is met, using the 2-way External Merge-Sort

        Args:
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.

                Operatores supported: (<,<=,==,>=,>)
                queries to run:
                select * from student inner join advisor on id=s_id

        '''

        # get columns and operator
        column_name_left, operator, column_name_right = self._parse_condition(condition, join=True)

        # get the column names so that they later match the condition
        # the left column name becomes left_table_name_name
        left_names = [f'{self._name}.{colname}'
                     if self._name!='' # if the table has a name
                     else
                     colname for colname in self.column_names # or it searches for a name in the columns
                     ]

        #likewise
        right_names = [f'{table_right._name}.{colname}' if table_right._name!='' else colname for colname in table_right.column_names]

        # define the new tables name, its column names and types
        join_table_name = ''
        join_table_colextras = self.column_extras + table_right.column_extras
        join_table_colnames = left_names+right_names
        join_table_coltypes = self.column_types+table_right.column_types
        # new version of the updated table
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes, column_extras=join_table_colextras)
        #Smj
        self.order_by(column_name_left,False) #sort the left table(self) by pk
        table_right.order_by(column_name_right,False) #sorting the right table by pk
        left_datalen = len(self.data) #the resulting records will be as many as the records in the left table,so we have to
        right_datalen = len(table_right.data) #find how many will be
        l_count, r_count = 0, 0 #starting from the begginning
        while l_count < left_datalen and r_count < right_datalen: #and for the length of the records
            if(self.column_by_name(column_name_left)[l_count] == table_right.column_by_name(column_name_right)[r_count]): # we check if the
                # records match in the column of the pk(left and right),in the current record of the counting counter
                join_table._insert(self.data[l_count]+table_right.data[r_count]) #then we put the result in the joined table(which takes space)
                l_count+=1 # in the current possition of the counter so that the records print in order
                r_count+=1 #then we increase both counter
            elif(self.data[l_count] < table_right.data[r_count]): #if one counter has surpased the other(cause maybe there were double records on the
                l_count+=1                 #column that we check)we bring the other counter right back up
            else:
                r_count+=1

        return join_table #and we return the table for printing