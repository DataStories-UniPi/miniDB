from __future__ import annotations
import shutil
from tabulate import tabulate
import pickle
import os
from miniDB.btree import Btree
from misc import get_op, split_condition


class Table:

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
        '''
        # get the column from its name

        column_idx = self.column_names.index(column_name)

        for i in range(len(self.data)):
            self.data[i][column_idx] = cast_type(self.data[i][column_idx])

        # change the type of the column
        self.column_types[column_idx] = cast_type


    def _insert(self, row, insert_stack=[]):\

        '''
        Insert row to table.
	  insert stack (empty by default).
        '''

        if len(row)!=len(self.column_names):
            raise ValueError(f'ERROR -> Cannot insert {len(row)} values. Only {len(self.column_names)} columns exist')

        for i in range(len(row)):
        
            row[i] = self.column_types[i](row[i])
           
            # check for no duplicate primary keys

            if i==self.pk_idx and row[i] in self.column_by_name(self.pk):
                raise ValueError(f'## ERROR -> Value {row[i]} already exists in primary key column.')

        # if insert_stack is not empty, append to its last index

        if insert_stack != []:
            self.data[insert_stack[-1]] = row
        else: 

		# else append to the end

            self.data.append(row)

    def _update_rows(self, set_value, set_column, condition):

        column_name, operator, value = self._parse_condition(condition)

        column = self.column_by_name(column_name)
        set_column_idx = self.column_names.index(set_column)

        # set_columns_indx = [self.column_names.index(set_column_name) for set_column_name in set_column_names]

        # for each value in column, if condition, replace it with set_value

        for row_ind, column_value in enumerate(column):
            if get_op(operator, column_value, value):
                self.data[row_ind][set_column_idx] = set_value

    def _delete_where(self, condition):
     
        column_name, operator, value = self._parse_condition(condition)

        indexes_to_del = []

        column = self.column_by_name(column_name)
        for index, row_value in enumerate(column):
            if get_op(operator, row_value, value):
                indexes_to_del.append(index)

        # we pop from highest to lowest index in order to avoid removing the wrong item

        for index in sorted(indexes_to_del, reverse=True):
            if self._name[:4] != 'meta':
                # if the table is not a metatable, replace the row with a row of nones
                self.data[index] = [None for _ in range(len(self.column_names))]
            else:
                self.data.pop(index)


    def _select_where(self, return_columns, condition=None, order_by=None, desc=True, top_k=None):

        # Select DISTINCT
        distinct = False

        # check if distinct is in return_columns (it means it comes after select)
        if 'distinct' in return_columns:
            distinct = True

            # remove distinct from return columns
            return_columns_tmp = return_columns.split('distinct')[1].strip().split(',')

            # select all the data in the table
            if '*' in return_columns_tmp:
                return_cols = [i for i in range(len(self.column_names))]

            # select specified columns
            else:
                return_cols = [self.column_names.index(col.strip()) for col in return_columns_tmp]

        # Basic select
        else:
            # return_cols = [self.column_names.index(col.strip()) for col in return_columns.split(',')]

            if return_columns == '*':
                return_cols = [i for i in range(len(self.column_names))]
            else:
                return_cols = [self.column_names.index(col.strip()) for col in return_columns.split(',')]

	'''
        if condition is None, return all rows
        if not, return the rows with values where condition is met for value
	'''
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

            else:
                column_name, operator, value = self._parse_condition(condition)
                column = self.column_by_name(column_name)

                rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
        else:
            rows = [i for i in range(len(self.data))]

	
        # copy the old dict, but only the rows and columns of data with index in rows/columns (the indexes that we want returned)

        dict = {(key):([[self.data[i][j] for j in return_cols] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}


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

            # check for multiplied data 
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

        # same as simple select 
        rows = rows[:top_k]

        dict = {(key):([[self.data[i][j] for j in return_cols] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}

        dict['column_names'] = [self.column_names[i] for i in return_cols]
        dict['column_types']   = [self.column_types[i] for i in return_cols]

        s_table = Table(load=dict) 
        if order_by:
            s_table.order_by(order_by, desc)

        s_table.data = s_table.data[:int(top_k)] if isinstance(top_k,str) else s_table.data

        return s_table

    def order_by(self, column_name, desc=True):

        column = self.column_by_name(column_name)
        idx = sorted(range(len(column)), key=lambda k: column[k], reverse=desc)

        self.data = [self.data[i] for i in idx]



    def _inner_join(self, table_right: Table, condition):

        # get columns and operator
        column_name_left, operator, column_name_right = self._parse_condition(condition, join=True)
        # try to find both columns, if you fail -> error
        try:
            column_index_left = self.column_names.index(column_name_left)
        except:
            raise Exception(f'Column "{column_name_left}" dont exist in left table. Valid columns: {self.column_names}.')

        try:
            column_index_right = table_right.column_names.index(column_name_right)
        except:
            raise Exception(f'Column "{column_name_right}" dont exist in right table. Valid columns: {table_right.column_names}.')

        # ex. for left -> name becomes left_table_name_name 
        left_names = [f'{self._name}.{colname}' if self._name!='' else colname for colname in self.column_names]
        right_names = [f'{table_right._name}.{colname}' if table_right._name!='' else colname for colname in table_right.column_names]

        # define the new tables name, its column names and types
        join_table_name = ''
        join_table_colnames = left_names+right_names
        join_table_coltypes = self.column_types+table_right.column_types
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)

        # count the number of operations 
        no_of_ops = 0
        for row_left in self.data:
            left_value = row_left[column_index_left]
            for row_right in table_right.data:
                right_value = row_right[column_index_right]
                no_of_ops+=1
                if get_op(operator, left_value, right_value): #EQ_OP
                    join_table._insert(row_left+row_right)

        return join_table


    def _inl_join(self, table_left:Table , table_right : Table , condition):
        
        #check for correct syntax of the query
       

        if table_left.pk is None  and table_right.pk is not None :
 
            index = Btree(3)
            for right_indx, key in enumerate(table_right.column_by_name(table_right.pk)):
                index.insert(key,right_indx)
        elif table_left.pk is not None and table_right.pk is None:

            # if the left table has the pk we must reverse them and use the one with the primary key 
            temp =table_right
            table_right = table_left
            table_left = temp
  
            index = Btree(3)
            for right_indx ,key in enumerate(table_right.column_by_name(table_right.pk)):
                index.insert(key,right_indx)

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

        column_name_left, operator, column_name_right = table_left._parse_condition(condition, join=True)
        if operator != '=':
            print("Wrong operator!!! You can only use the '=' operator")
            return
        # try to find both columns, if you fail -> error
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
      
        print(tabulate(non_none_rows[:no_of_rows], headers=headers)+'\n')


    def _parse_condition(self, condition, join=False):

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
            self.cut_file(file)
            with open('tempFiles/1','r') as last_table_read:
                for line in last_table_read:
                    final_list.append(line)
            last_table_read.close() 
            return final_list

        # cuts original file into smaller pieces. Each piece has 3 values
        def cut_file(self,original_file):
            flag = True
            # counter for lines
            current_index = 0 
            # the number of total temp files
            number_of_files = 1
            with open('tempFiles/'+str(original_file), 'r') as read_obj:
                cut_values = []
                # Line by line copy data from original file to dummy file
                for line in read_obj:
                    # If current line number matches the given line number then skip copying
                    if current_index <= 2:
                        cut_values.append(line)
                        # if we have 3 lines we sort them and write them in files
                        if current_index == 2:
                            cut_values.sort()
                            # write the 3 lines in a new temp file
                            with open('tempFiles/'+str(number_of_files), 'w+') as write_obj:
                                for i in range(0,len(cut_values)):
                                    if cut_values[i]!="\n":
                                        write_obj.write(str(cut_values[i]))
                                number_of_files+=1
                            write_obj.close()
                            current_index = 0
                            cut_values = []
                        else:    
                            current_index += 1
                # if there are remaining lines when the loop stops, sort them and save the in another temp file
                if len(cut_values) != 0:
                    flag=False;
                    cut_values.sort()
                    with open('tempFiles/'+str(number_of_files), 'w+') as write_obj:
                        for i in range(0,len(cut_values)):
                            if cut_values[i]!="\n":
                                write_obj.write(str(cut_values[i]))
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

            leftList=[]
            rightList=[]
            # this will store left+right lists
            combinedList=[]

            for i in range(0,int(number_of_files/2)):
                leftList=[]
                rightList=[]
                combinedList=[]

                # reads cuted files and appends them in left and right lists
                with open('tempFiles/'+str(left_counter),'r') as leftRead_obj, open('tempFiles/'+str(left_counter+1),'r') as rightRead_obj:
                    for line1 in leftRead_obj:
                        leftList.append(line1)
                    for line2 in rightRead_obj:
                        rightList.append(line2)
                leftRead_obj.close()
                rightRead_obj.close()

                # the combined list of left and right list is sorted with merge-sort algorithm
                combinedList = self.mergeSortAlgorithm(leftList,rightList)

                # write the combined list into a temp file
                with open('tempFiles/'+str(file_counter), 'w+') as write_obj:
                    for i in range(0,len(combinedList)):
                        write_obj.write(str(combinedList[i]))
                write_obj.close()
                left_counter+=2 
                file_counter+=1 


            # if number_of_files is ODD we have one file remaining so we combine it with the last merged file
            if number_of_files%2!=0:
                leftList=[]
                rightList=[]
                combinedList=[]

                # we open the last merged file and the file remaining
                with open('tempFiles/'+str(file_counter-1), 'r') as leftRead_obj, open('tempFiles/'+str(left_counter),'r') as rightRead_obj:
                    for line1 in leftRead_obj:
                        leftList.append(line1)
                    for line2 in rightRead_obj:
                        rightList.append(line2)
                leftRead_obj.close()
                rightRead_obj.close()

                # the combined list of left and right list is sorted with merge-sort algorithm
                combinedList = self.mergeSortAlgorithm(leftList,rightList)

                # write the combined list into a temp file
                with open('tempFiles/'+str(file_counter-1), 'w+') as write_obj:
                    for i in range(0,len(combinedList)):
                        write_obj.write(str(combinedList[i]))
                write_obj.close()

            ''' 
                file_counter = number_of_files + 1. If file_counter >= 3 then number_of_files >= 2 so we call again mergeSortedFiles function 
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
        # Remember: we have already cuted our file so we use 2 different cuts for the algorithm
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
