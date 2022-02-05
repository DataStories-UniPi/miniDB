from __future__ import annotations
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


    def _select_where(self, return_columns, condition=None, order_by=None, desc=True, top_k=None,group_by_col_list=None,aggr_func_list=None,having_condition=None):
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
        return_columns = return_columns.replace(' ','')
        
        if return_columns == '*':
            return_cols = [i for i in range(len(self.column_names))]
            return_columns = [col for col in self.column_names]
        elif aggr_func_list is not None :
            a = 5 
            b = 3.5   
            return_columns = [i for i in aggr_func_list]
            for func in aggr_func_list : 
              if func.strip().startswith("avg") | func.strip().startswith("sum") :
                if self.column_types[self.column_names.index(self.search_betweens(func,"(",")"))] != type(a) and  self.column_types[self.column_names.index(self.search_betweens(func,"(",")"))] != type(b) :
                   print("Functions Sum and Average only use numeric types")
                   return False 
            for col in group_by_col_list :
               return_columns.append(col)
            print(return_columns)
            return_cols =[]
            for col in return_columns:
               return_cols.append(return_columns.index(col))
        else:
            return_cols = [self.column_names.index(col.strip()) for col in return_columns.split(',')]
            return_columns = [col for col in return_columns.split(',')]
        

         
        # if condition is None, return all rows
        # if not, return the rows with values where condition is met for value
        if condition is not None:
            column_name, operator, value = self._parse_condition(condition,return_columns,False)
            column = self.column_by_name(column_name)
            rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
        else:
            rows = [i for i in range(len(self.data))]
        if group_by_col_list is not None : 
          self.data_before_print = self.data
          print(self.data)
          self.data = self.group_by_func(group_by_col_list,aggr_func_list,rows)
          if having_condition is not None:
            ret = [] 
            for col in return_columns :
               ret.append(col.replace(' ',''))
            column_name, operator, value = self._parse_condition(having_condition,ret,False)
            column = [row[ret.index(column_name)] for row in self.data]
            rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
          else :
            rows = [i for i in range(len(self.data))]
        # top k rows
        # rows = rows[:int(top_k)] if isinstance(top_k,str) else rows
        # copy the old dict, but only the rows and columns of data with index in rows/columns (the indexes that we want returned)
        dict = {(key):([[self.data[i][j] for j in return_cols] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}
        
        # we need to set the new column names/types and no of columns, since we might
        # only return some columns
        dict['column_names'] = [i for i in return_columns]
        dict['column_types']   = [self.column_types[i] for i in return_cols]
        #dict['column_names'] = [self.column_names[i] for i in return_cols]
        #dict['column_types']   = [self.column_types[i] for i in return_cols]
        
        s_table = Table(load=dict) 
        
        if order_by:
            s_table.order_by(order_by, desc)

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


    def show(self, no_of_rows=None, is_locked=False,group_by = None):
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
        '''
           The data of the table is restored in case an aggregate function changes it 
        '''
        if group_by is not None : 
          self.data = self.data_before_print

    def _parse_condition(self, condition, return_columns=None, join=False):
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
        left = left.replace(' ','')
        if left not in self.column_names and left not in return_columns :
            raise ValueError(f'Condition is not valid (cant find column name)')
        if return_columns is not None :
          if left in return_columns and left not in self.column_names :
            left_col_in = self.search_betweens(left,"(",")") 
            coltype = self.column_types[self.column_names.index(left_col_in)]
          else :
            coltype = self.column_types[self.column_names.index(left)]
        else :
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
        
    def search_betweens(self,s, first, last): 
     '''
     Search in 's' for the substring that is between 'first' and 'last'
     '''
     try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
     except:
        return
     return s[start:end].strip()
          
    def group_by_func(self,group_by_col_list,aggr_func_list,rows):
        '''
        groups all the distinct combinations in comb_list and then proceeds to the aggregate function.
        for example ,
              we have the columns year,month,days and in the group by clause there is count(days)
              and the values are row1 : year = 2001 ,month = 12 ,days = 15
              row2 : year = 2001 ,month = 12 ,days = 17
              row3 : year = 2003 ,month = 10 ,days = 25 
              The comb_list will be [[2001,12][2003,10]].
        '''
        self.group_by_value_lists = []
        self.comb_list = []
        for row in rows :
           self.group_by_distinct = []
           for col in group_by_col_list :
             self.group_by_distinct.append(self.data[row][self.column_names.index(col)])
           if self.group_by_distinct not in self.comb_list :
             self.comb_list.append(self.group_by_distinct)
        print('comb list : ', self.comb_list)
           
        for func in aggr_func_list :
           if func.strip().startswith("count") :
             self.comb_list = self.count(group_by_col_list,rows)
           elif func.strip().startswith("max") :
             self.comb_list = self.max(group_by_col_list,rows,func)
           elif func.strip().startswith("min") :
             self.comb_list = self.min(group_by_col_list,rows,func)
           elif func.strip().startswith("avg") :
             self.comb_list = self.avg(group_by_col_list,rows,func)
           elif func.strip().startswith("sum") : 
             self.comb_list = self.sum(group_by_col_list,rows,func)
        return self.comb_list     
        
    def count(self,group_by_col_list,rows) : 
        '''
         At the end of this function and every other aggregate function the count or sum etc is added at the end of every comb.
         for example,
              lets say the count of days for the comb : [2001,12] is 2 and for the comb : [2003,10] its 1.at the end of this function the comb list
              will look like this : [[2001,12,2],[2003,10,1]].
              And then this comb_list will return to the select_where function and replace self.data temporarily.
        '''
        #col_in_func = mdb.search_between(func,"(",")")
        data = []
        self.count_of_col_in_combs = [0 for i in range(len(self.comb_list))]
        for row in rows :
          self.group_by_distinct = []
          for col in group_by_col_list : 
             self.group_by_distinct.append(self.data[row][self.column_names.index(col)]) 
          for comb in self.comb_list : 
             if self.group_by_distinct == comb : 
                index = self.comb_list.index(comb) 
                self.count_of_col_in_combs[index] += 1
        for idx in range(len(self.comb_list)) : 
             self.comb_list[idx].insert(0,self.count_of_col_in_combs[idx])
        return self.comb_list
        
    def max(self,group_by_col_list,rows,func) :
        col_in_func = self.search_betweens(func,"(",")")  
        col_in_func = col_in_func.strip()
        max_val_list = [None for i in range(len(self.comb_list))]
        for row in rows :
          self.group_by_distinct = []
          for col in group_by_col_list : 
             self.group_by_distinct.append(self.data[row][self.column_names.index(col)]) 
          for idx,comb in enumerate(self.comb_list) : 
             if self.group_by_distinct == comb :
               max_val_list[idx] = self.data[row][self.column_names.index(col_in_func)]
        for row in rows :
          self.group_by_distinct = []
          for col in group_by_col_list : 
             self.group_by_distinct.append(self.data[row][self.column_names.index(col)]) 
          for idx,comb in enumerate(self.comb_list) : 
             if self.group_by_distinct == comb :
               if self.data[row][self.column_names.index(col_in_func)] > max_val_list[idx] :
                 max_val_list[idx] = self.data[row][self.column_names.index(col_in_func)]
        for idx in range(len(self.comb_list)) : 
             self.comb_list[idx].insert(0,max_val_list[idx])
        return self.comb_list
        
    def min(self,group_by_col_list,rows,func) :   
        col_in_func = self.search_betweens(func,"(",")")  
        col_in_func = col_in_func.strip()
        min_val_list = [None for i in range(len(self.comb_list))]
        for row in rows :
          self.group_by_distinct = []
          for col in group_by_col_list : 
             self.group_by_distinct.append(self.data[row][self.column_names.index(col)]) 
          for idx,comb in enumerate(self.comb_list) : 
             if self.group_by_distinct == comb :
               min_val_list[idx] = self.data[row][self.column_names.index(col_in_func)]
        for row in rows :
          self.group_by_distinct = []
          for col in group_by_col_list : 
             self.group_by_distinct.append(self.data[row][self.column_names.index(col)]) 
          for idx,comb in enumerate(self.comb_list) : 
             if self.group_by_distinct == comb :
               if self.data[row][self.column_names.index(col_in_func)] < min_val_list[idx] :
                 min_val_list[idx] = self.data[row][self.column_names.index(col_in_func)]
        for idx in range(len(self.comb_list)) : 
             self.comb_list[idx].insert(0,min_val_list[idx])
        return self.comb_list
        
    def avg(self,group_by_col_list,rows,func) :  
         col_in_func = self.search_betweens(func,"(",")")  
         col_in_func = col_in_func.strip() 
         avg_val_list = [[0,0] for i in range(len(self.comb_list))]
         for row in rows :
          self.group_by_distinct = []
          for col in group_by_col_list : 
             self.group_by_distinct.append(self.data[row][self.column_names.index(col)]) 
          for idx,comb in enumerate(self.comb_list) : 
             if self.group_by_distinct == comb :
               avg_val_list[idx] = [avg_val_list[idx][0] + self.data[row][self.column_names.index(col_in_func)],avg_val_list[idx][1] + 1]
         for idx in range(len(avg_val_list)) :
            avg_val_list[idx] = avg_val_list[idx][0] // avg_val_list[idx][1]
         
         for idx in range(len(self.comb_list)) : 
             self.comb_list[idx].insert(0,avg_val_list[idx])
         print(self.comb_list)
         return self.comb_list 
      
    def sum(self,group_by_col_list,rows,func) : 
         col_in_func = self.search_betweens(func,"(",")")  
         col_in_func = col_in_func.strip() 
         sum_val_list = [0 for i in range(len(self.comb_list))]
         for row in rows :
          self.group_by_distinct = []
          for col in group_by_col_list : 
             self.group_by_distinct.append(self.data[row][self.column_names.index(col)]) 
          for idx,comb in enumerate(self.comb_list) : 
             if self.group_by_distinct == comb :
               sum_val_list[idx] = sum_val_list[idx] + self.data[row][self.column_names.index(col_in_func)]
         for idx in range(len(self.comb_list)) : 
          self.comb_list[idx].insert(0,sum_val_list[idx])
         return self.comb_list  
        
         
