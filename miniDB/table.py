from __future__ import annotations
from typing import Tuple
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

    def group_by_having(self, return_columns, condition, group_by_columns, having_condition=None, order_by=None, desc=True, top_k=None):
        
        '''
        SQL SYNTAX CHECK
        '''
        #all columns/aggregates of select and group by. We store aggregates as tuples (col_idx, aggregate function)
        group_by_cols = [self.column_names.index(col.strip()) if col.strip() in self.column_names else self.aggr_idx(col.strip()) for col in group_by_columns.split(",")]
        return_cols   = [self.column_names.index(col.strip()) if col.strip() in self.column_names else self.aggr_idx(col.strip()) for col in return_columns.split(",")]

        if any(isinstance(c, tuple) for c in group_by_columns): #if we have aggregates in group by
            raise Exception("Aggregate functions are not allowed in group by clause")
        for col in return_cols:      #if we have to select columns that are not in group by
            if not(isinstance(col, tuple)) and not col in group_by_cols:
                raise Exception(f'{self.column_names[col]} must appear in group by clause or be used in an aggregate function')
        
        #Find all distinct lists with n elements in table(these are the groups), where n is the count of group by columns
        #Depending on the where condition, we decide if we are going to put the n list in the groups or not 
        groups = []

        if condition is not None:
            where_column_name, where_operator, where_value = self._parse_condition(condition)   #name of the where column, operator and value

        for row in range(len(self.data)):
            if condition is not None:
                col_idx = self.column_names.index(where_column_name)
                test_group = [self.data[row][col] for col in group_by_cols] if get_op(where_operator, self.data[row][col_idx] , where_value) else []
            else:
                test_group = [self.data[row][col] for col in group_by_cols]
            if not test_group in groups and test_group != []:
                groups.append(test_group)

        if having_condition is not None : #get the tuple (idx, aggregate) for the having aggregate, the operator and value
            having_aggr, having_operator, having_value = self._parse_condition(having_condition, having=True)
            try:
                having_value = int(having_value)
            except:
                raise Exception("Having condition has to compare aggregate with int")
            having_aggregate = self.aggr_idx(having_aggr)

            #Modify the groups list so it will contain only the groups that satisfy the having condition
            groups_to_remove = []
            for group in groups:
                #We call the function that has the name of the aggregate with the corresponding parameters
                aggr_result = getattr(self, having_aggregate[1])(having_aggregate[0], condition, group)
                if not get_op(having_operator, aggr_result, having_value):
                    groups_to_remove.append(group)
            for group in groups_to_remove:
                groups.remove(group)
        
        result_data = []
        result_names = []
        result_types = []
        #if we have to select only columns and then group by
        #that means, that we cannot instansiate any tuples, since tuples contain the indeces for aggregates.
        if not any(isinstance(col, tuple) for col in return_cols):      
            for row in groups:
                result_row = []
                #in this for loop, we assure that we are looking at the correct column. Because there could be different select columns in SELECT command and different columns in GROUP BY command.
                for col in return_cols:
                    idx_in_group_by = group_by_cols.index(col)
                    result_row.append(row[idx_in_group_by])
                                
                result_data.append(result_row)
            result_names = [self.column_names[idx] for idx in return_cols]
            result_types = [self.column_types[idx] for idx in return_cols]
                
        #if we have to select only aggregates and then group by, which means that ALL are tuples in return_cols (the first element is the row and the second is the agg. function)
        elif all(isinstance(col, tuple) for col in return_cols):
            #for every group that's been made:
            for group in groups:
                result_row = []
                #check every column and its aggregate function. Then call said aggregate function.
                for col in return_cols:
                    #make the appropriate name for every row. The type will be an INT if we have count avg or sum. Otherwise it will stay as it was.
                    result_names.append(f'{col[1]}({self.column_names[col[0]]})')
                    result_types.append(int if col[1] in ['count', 'avg', 'sum'] else self.column_types[col])

                    if col[1] != 'avg':
                        row_data = getattr(self, col[1])(col[0], condition, group)
                        #except if it is AVG() then you have to call count and sum independently.
                    else:
                        row_data = self.count(col[0], condition, group) / self.sum(col[0], condition, group)
                    
                    #then append that row to our data.
                    result_row.append(row_data)
                
                #for every group that we made, make an entire column.
                result_data.append(result_row) 

        #if we have to select aggregates and columns and then group by
        else:
            for group in groups:
                result_row = []
                for col in return_cols:
                    #if we have an aggregate function, then we call that aggregate function first.
                    #the code from this point and on is the same as above, as we do all the same stuff for an agg function.
                    if isinstance(col, tuple):
                        result_names.append(f'{col[1]}({self.column_names[col[0]]})')
                        result_types.append(int if col[1] in ['count', 'avg', 'sum'] else self.column_types[col])

                        if col[1] != 'avg':
                            row_data = getattr(self, col[1])(col[0], condition, group)
                        else:
                            row_data = self.count(col[0], condition, group) / self.sum(col[0], condition, group)

                        result_row.append(row_data)

                    #otherwise, we have a simple column to show.
                    else:
                        idx_in_group_by = group_by_cols.index(col)
                        result_row.append(group[idx_in_group_by])

                        result_names.append(self.column_names[col])
                        result_types.append(self.column_types[col])

                result_data.append(result_row)


        dict = {(key):(result_data if key == "data" else value) for key,value in self.__dict__.items()} #data has only the result row
        dict['column_names'] = result_names
        dict['column_types'] = result_types   

        s_table = Table(load=dict) 
        if order_by:
            s_table.order_by(order_by.replace(" ", ""), desc)

        s_table.data = s_table.data[:int(top_k)] if isinstance(top_k,str) else s_table.data

        return s_table
        

    def count(self, col_idx, condition, group = []):        #returns the sql count(column) function result as int

        count_rows = 0

        if group != []: #if we have to count column values depending on a group of values at each row
            
            counted_column_values = self.column_by_name(self.column_names[col_idx]) #list with the counted column's values
            
            if condition is not None:   #if we have also a where condition to check
                condition_column_name, operator, value = self._parse_condition(condition)   #name of the where column, operator and value
                condition_column_values = self.column_by_name(condition_column_name)        #list with all the condition's column values
                for row in range (len(self.data)):
                    if all(val in self.data[row] for val in group) and counted_column_values[row] != 'null' and get_op(operator, condition_column_values[row], value):
                        count_rows += 1         
            else:   #if we dont have a where condition to check
                for row in range (len(self.data)):
                    if all(val in self.data[row] for val in group) and counted_column_values[row] != 'null':
                        count_rows += 1
        elif condition is not None: #if we have only a where condition to check 
            condition_column_name, operator, value = self._parse_condition(condition)   #name of the where column, operator and value
            condition_column_values = self.column_by_name(condition_column_name)        #list with all the condition's column values
            
            '''
            We check one by one the condition values in the condition column. if they satisfy the condition and
            the corresponding column value in the counted column is not null we add one to the counter
            
            '''
            for idx, val in enumerate(condition_column_values): 
                if get_op(operator, val, value) and self.data[idx][col_idx] != 'null':
                    count_rows += 1
        else:   #if we dont have any condition
            '''
            We check one by one the values of the counted column and if they are not null, we count them
            
            '''
            counted_column_values = self.column_by_name(self.column_names[col_idx]) #list with the counted column's values

            #if the value is not null, then count it.
            for val in counted_column_values:
                if val != 'null':
                    count_rows += 1
        return count_rows

    def sum(self, col_idx, condition, group = []):

        #as a typical sum function, there must be a starting value of 0.
        sum_of_rows = 0

        if group != []:
            counted_column_values = self.column_by_name(self.column_names[col_idx]) #list with the counted column's values

            #WHERE AND GROUPS
            if condition is not None:

                #this is the WHERE part.
                condition_column_name, operator, value = self._parse_condition(condition)   #name of the where column, operator and value
                condition_column_values = self.column_by_name(condition_column_name)        #list with all the condition's column values

                for row in range (len(self.data)):
                    if all(val in self.data[row] for val in group) and counted_column_values[row] != 'null' and get_op(operator, condition_column_values[row], value):
                        try:
                            sum_of_rows += self.data[row][col_idx]
                        except:
                            raise Exception("ERROR: There must be an int involved in SUM() functions. Aborting.")
            
            #if we don't have WHERE.
            else:
                for row in range (len(self.data)):
                    if all(val in self.data[row] for val in group) and counted_column_values[row] != 'null':
                        try:
                            sum_of_rows += self.data[row][col_idx]
                        except:
                            raise Exception("ERROR: There must be an int involved in SUM() functions. Aborting.")
        
        #ONLY WHERE.
        elif condition is not None:
            condition_column_name, operator, value = self._parse_condition(condition)
            condition_column_values                = self.column_by_name(condition_column_name)

            #we enumerate every single column. If it satisfies the condition, and also not null, we can sum it.
            for idx, val in enumerate(condition_column_values):
                if get_op(operator, val, value) and self.data[idx][col_idx] != 'null':
                    try:
                        sum_of_rows += self.data[idx][col_idx]
                    except:
                        raise("ERROR: There must be an int involved in SUM() functions. Aborting.")

        #NEITHER WHERE NOR GROUPS (all)
        else:
            counted_column_values = self.column_by_name(self.column_names[col_idx])

            for val in counted_column_values:
                if val != 'null':
                    try:
                        sum_of_rows += val
                    except:
                        raise("ERROR: There must be an int involved in SUM() functions. Aborting.")

        return sum_of_rows

    def min(self, col_idx, condition, group = []):

        #if there is a condition to check, we get the name of the WHERE column the operator (<, >, etc.) and the value provided.
        if condition is not None:
            condition_column_name, operator, value = self._parse_condition(condition)
            condition_column_values                = self.column_by_name(condition_column_name)

            #here the enumeration gets done a little differently. We do all the same as above with the conditions, execpt we store all the data to a list and immideately taking the minimum element.
            minimum = 'null'
            minimum = min([(idx, val) for idx, val in enumerate(condition_column_values) if get_op(operator, val, value) and self.data[idx][col_idx] != 'null'])

        else:
            #same here, except that we only count the non null values, since there is no condition to check.
            counted_column_values = self.column_by_name(self.column_names[col_idx])
            minimum = 'null'
            minimum = min([val for val in counted_column_values if val != 'null'])

        return minimum
    
    def max(self, col_idx, condition, group = []):

        #everything here is the same as minimum, except there is the max() statement.
        if condition is not None:
            condition_column_name, operator, value = self._parse_condition(condition)
            condition_column_values                = self.column_by_name(condition_column_name)

            maximum = 'null'
            maximum = max([(idx, val) for idx, val in enumerate(condition_column_values) if get_op(operator, val, value) and self.data[idx][col_idx] != 'null'])

        else:
            counted_column_values = self.column_by_name(self.column_names[col_idx])
            maximum = 'null'
            maximum = max([val for val in counted_column_values if val != 'null'])

        return maximum
            

    #returns a tuple of the index column and the aggregate function of the given aggregate
    def aggr_idx(self, aggregate):

        col = aggregate.split('(')[1][:-1].strip()
        aggregate = aggregate.split('(')[0].strip()

        if col not in self.column_names:
            raise Exception("Column " + str(col) + " cannot be found")

        if aggregate not in ["min", "max", "sum", "avg", "count"]:
            raise Exception(f"Incorrect aggregate {aggregate}")

        col = self.column_names.index(col)

        return (col, aggregate)

    def select_aggr(self, aggregates, condition=None, order_by=None, desc=True, top_k=None):
        '''
        Substitute for select. This function gets run when the user uses one or multiple aggregate functions in select (e.g. select count(id), max(salary)).
        '''
        result_row = []     #the values of the row we are going to show
        result_names = []   #the names of the rows we are going to show

        for aggregate_function in aggregates:

            if (aggregate_function[1] == 'sum' or aggregate_function[1] == 'avg') and self.column_types[aggregate_function[0]] != int:
                    print(f'ERROR: Column "{self.column_names[aggregate_function[0]]}" is a type of {str(self.column_types[aggregate_function[0]])}. {aggregate_function[1]} is only callable in {int} types.')
                    return None
            if aggregate_function[1] == 'avg':
                aggr_result = self.sum(aggregate_function[0], condition) / self.count(aggregate_function[0], condition)
            else:
                aggr_result = getattr(self, aggregate_function[1])(aggregate_function[0], condition)
            result_row.append(aggr_result)
            result_names.append(f'{aggregate_function[1]}({self.column_names[aggregate_function[0]]})')

            '''#we call count function if we have to count a column
            if aggregate_function[1] == 'count':    
                count = self.count(aggregate_function[0], condition)
                result_row.append(count)
                result_names.append(f'count({self.column_names[aggregate_function[0]]})')

            #if the aggregate function the user called is sum
            elif aggregate_function[1] == 'sum':

                #first we check if the column that the aggregate function was called is an int. SUM is only callable in ints.
                if self.column_types[aggregate_function[0]] != int:
                    print(f'ERROR: Column "{self.column_names[aggregate_function[0]]}" is a type of {str(self.column_types[aggregate_function[0]])}. SUM is only callable in {str(type(0))} types.')
                    return None
                
                #if the value is int, then we can call calculate_sum, which is roughly the same as count()
                sum = self.sum(aggregate_function[0], condition)
                result_row.append(sum)
                result_names.append(f'sum({self.column_names[aggregate_function[0]]})')

            elif aggregate_function[1] == 'avg':
                
                #average also needs to be done only on int values
                if self.column_types[aggregate_function[0]] != int:
                    print(f'ERROR: Column "{self.column_names[aggregate_function[0]]}" is a type of {str(self.column_types[aggregate_function[0]])}. AVG is only callable in {str(type(0))} types.')
                    return None
                
                #average is the sum divided by the count. The point is that these things already exist as functions. so we use those two functions simultaneously.
                avg = self.sum(aggregate_function[0], condition) / self.count(aggregate_function[0], condition)
                result_row.append(avg)
                result_names.append(f'avg({self.column_names[aggregate_function[0]]})')
            
            #minimum and maximum functions don't have int constraints. So they're executed immediately.
            elif aggregate_function[1] == 'min':
                
                minimum = self.min(aggregate_function[0], condition)
                result_row.append(minimum)
                result_names.append(f'min({self.column_names[aggregate_function[0]]})')

            elif aggregate_function[1] == 'max':

                maximum = self.max(aggregate_function[0], condition)
                result_row.append(maximum)
                result_names.append(f'max({self.column_names[aggregate_function[0]]})')'''

        #Set the new attribute dictionary. We change the data attribute, the column names and the types
        dict = {(key):([result_row] if key=="data" else value) for key,value in self.__dict__.items()} #data has only the result row
        dict['column_names'] = result_names
        dict['column_types'] = [int for _ in result_row]    #all the aggregate functions return int

        s_table = Table(load=dict) 
        if order_by:
            s_table.order_by(order_by.replace(" ", ""), desc)

        s_table.data = s_table.data[:int(top_k)] if isinstance(top_k,str) else s_table.data

        return s_table

    def _select_where(self, return_columns, condition=None, group_by_columns=None, having_condition=None, order_by=None, desc=True, top_k=None):
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
        if return_columns == '*':   #in case we have to select all columns, we dont have to check for group by keyword(not applicable in this case)
            return_cols = [i for i in range(len(self.column_names))]
        elif group_by_columns is None:  #in this case we only have some columns/aggregate functions to select, but not a group by keyword
            #we have to check if we have only aggregate functions or columns. Both cannot be selected without group by
            aggr_keys=['min','max','avg','count','sum'] #list of aggregate keywords
            for k in aggr_keys:
                if any(c.strip().startswith(k) for c in return_columns.split(',')) and any(c.strip() in self.column_names for c in return_columns.split(',')):
                    raise Exception("Group by clause is missing") #if both column and aggregate can be found, raise exception
            #at this point we have only aggregates or columns.
            return_cols = [(self.column_names.index(col.strip()) if col.strip() in self.column_names else self.aggr_idx(col.strip())) for col in return_columns.split(',')]

            #then we have to check if we have only aggregate functions in order to select the aggregate values
            if isinstance(return_cols[0], tuple): #if the first element is aggregate all the elements are aggregate 
                return self.select_aggr(return_cols, condition, order_by, desc, top_k)

        else:
            return self.group_by_having(return_columns, condition, group_by_columns, having_condition, order_by, desc, top_k)
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
        dict = {(key):([[self.data[i][j] for j in return_cols] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}

        # we need to set the new column names/types and no of columns, since we might
        # only return some columns
        dict['column_names'] = [self.column_names[i] for i in return_cols]
        dict['column_types'] = [self.column_types[i] for i in return_cols]

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



    def _parse_condition(self, condition, join=False, having=False):
        '''
        Parse the single string condition and return the value of the column/aggregate and the operator.

        Args:
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.
                
                Operatores supported: (<,<=,==,>=,>)
            join: boolean. Whether to join or not (False by default).
            having: boolean. Whether we have having or not (False by default).
        '''
        # if both_columns (used by the join function) return the names of the names of the columns (left first)
        # Also if we have a having condition, return the aggregate function the operator and the value
        if join or having:            
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
