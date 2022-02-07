from __future__ import annotations
from tabulate import tabulate
import pickle
import os

from misc import get_op, split_condition


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

    def _group_by_method(self,group_by,all_rows,all_in,having):



        #Analyetai to orisma group_by kai apothikeyontai oi sthles toy sthn parakatw lista
        group_columns=[self.column_names.index(j.strip()) for j in group_by.split(',')]

        results=[]
        dic={}
        #Trexoume tis for gia kathe row toy pinaka kathws kai kathe sthlh apo to group by wste na apothikeytoun san key ta katallhla pedia me ta antistoixa rows
        for i in all_rows:
            record=[]
            for j in group_columns:
                record.append(self.data[i][j])
            if tuple(record) in dic :
                dic[tuple(record)].append(i)
            else:
                dic[tuple(record)]=[i]

        if having:

            dic=self.having(having,dic)

        for value in dic.values():
            temporary=[]
            for a in all_in['sum']:
                temporary.insert(a[0],sum([self.data[i][a[1]] for i in value]))
            for b in all_in['avg']:
                temporary.insert(b[0],sum([self.data[i][b[1]] for i in value])/len(value))
            for c in all_in['count']:
                temporary.insert(c[0],len(value))
            for d in all_in['min']:
                temporary.insert(d[0],min([self.data[i][d[1]] for i in value]))
            for f in all_in['max']:
                temporary.insert(f[0], max([self.data[i][f[1]] for i in value]))
            for e in all_in['None']:
                temporary.insert(e[0],self.data[value[0]][e[1]])
            for z in all_in['*']:
                ind=z[0]
                for column in z[1]:
                    temporary.insert(ind,self.data[value[0]][column])
                    ind+=1


            results.append(temporary)

        return results




    def having(self,having,dic):

        #To orisma having analyetai sthn sthlh poy epithymoume na ginei to check,ton operator kathws kai thn timh pou ginetai h sygkrish
        element=having.strip(" ")
        column = self.column_names.index(element[element.find("(") + 2:element.find(")") - 1])
        op=element[element.find(")")+2]
        value=element[element.find(")")+3:]

        #Ean h timh einia typou number tote metatrepetai se float
        
        try:
            value=float(value)
        except ValueError:
        	pass

        keys_toremove=[]#xrhsimeyei gia na diagrapsoyme ta keys poy den ikanopoioun thn synthiki tou having
        for key,val in dic.items():

            #Se kathe epanalipsi epanw sto key kai to value tou dictionary, ean den ikanopoieitai h synthiki tou operator(se sxesh me tis times toy key kai to value poy ypologisame nwritera)

            #tote to sygkekrimeno key prostithetai sthn lista keys_toremove wste na diagrafei sthn synexia apo to dictionary
            if element.startswith("sum ("):
                temp=sum([self.data[i][column] for i in val])

                if not get_op(op, temp, value):
                    keys_toremove.append(key)
            elif element.startswith("count ("):
                 temp=len(value)
                 if not get_op(op, temp, value):
                     keys_toremove.append(key)
            elif element.startswith("avg ("):
                temp = sum([self.data[i][column] for i in val])
                temp=temp/len(val)
                if not get_op(op, temp, value):
                    keys_toremove.append(key)
            elif element.startswith("max ("):
                tempmax=max([self.data[i][column] for i in val])
                if not get_op(op, tempmax, value):
                    keys_toremove.append(key)
            elif element.startswith("min ("):
                tempmin=min([self.data[i][column] for i in val])
                if not get_op(op, tempmin, value):
                    keys_toremove.append(key)


        for key in keys_toremove:
            del dic[key]


        return dic








    def aggregate(self,all_in,all_rows,all_columns):
        data=[]
        minmax=[]

        if all_in['min'] :
            final=[]
            for sub in all_in['min']:
                temp=[self.data[i][sub[1]] for i in all_rows ]

                final=temp[:]
                data.insert(sub[0],min(temp))

            if len(final)!=0:

                index_the_row=final.index(min(final))
                minmax.append([self.data[index_the_row][j] for j in all_columns])


        if all_in['max'] :
            final=[]
            for sub in all_in['max']:
                temp = [self.data[i][sub[1]] for i in all_rows]
                final=temp[:]
                data.insert(sub[0], max(temp))

            if len(final)!=0:

                 index_the_row = final.index(max(final))
                 minmax.append([self.data[index_the_row][j] for j in all_columns])

        if all_in['sum'] :

                for sub in all_in['sum']:



                    temp = [self.data[i][sub[1]] for i in all_rows]

                    data.insert(sub[0], sum(temp))




        if all_in['avg'] :
            for sub in all_in['avg']:
                temp = [self.data[i][sub[1]] for i in all_rows]
                data.insert(sub[0], sum(temp)/len(all_rows))

        if all_in['count'] :
            for sub in all_in['count']:
                data.insert(sub[0],len(all_rows))

        if all_in['*'] :
            for sub in all_in['*']:


                if len(minmax)==0:

                    temp=[self.data[0][j] for j in sub[1]]
                    data.insert(sub[0],temp)
                else:
                    data.insert(sub[0],minmax[-1])

        if all_in['None'] :
            for sub in all_in['None']:

                if len(minmax) == 0:
                    data.insert(sub[0],self.data[0][sub[1]])
                else:
                    data.insert(sub[0],minmax[-1][sub[1]])


        return [self.unpacking(data)]



    #Xrhsimeyei  gia na eksagontai ta stoixeia apo kapoies listes (eswterika kapoias allhs listas)

    def unpacking(self,array):

        endlist=[]
        for element in array:

            if type(element) is list:
                for el in element:
                    endlist.append(el)
            else:
                endlist.append(element)

        return endlist


    def multiple_where_clause(self,condition):


        #iterate through condition(list) and if we meet an AND condition we intersect lists before and after (AND condition) otherwise we combine them
        #return the rows with values where condition is met for value removing the duplicates

        column_name, operator, value = self._parse_condition(condition[0])
        column = self.column_by_name(column_name)
        row = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
        for i in range(1,len(condition)-1,2):
            column_name, operator, value = self._parse_condition(condition[i+1])
            column = self.column_by_name(column_name)
            temprow = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]
            if condition[i].lower()=='and':
                row=list(set(row) & set(temprow))
            elif condition[i].lower()=='or':
                row=row+temprow

        row=sorted(list(set(row)))
        return row





    def _select_where(self, return_columns, condition=None, group_by=None,having=None,order_by=None, desc=True, top_k=None):
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

        #Using dictionary for aggregate function



        #We need all columns and rows for aggregate funcion



        all_columns=[i for i in range(len(self.column_names))]

        all_in_on = {'sum': [], 'count': [], 'avg': [], 'max': [], 'min': [], 'None': [], '*': []}
        # Ean to return_collumns einai iso  me * tote epistrefontai oles oi sthles
        #Diaforetika xrhsimopoioume to apo panw  dictionary all_in_on wste na mporoun na ikanopoihthoun kai aithmata ths morfhs p.x
        #select *,*,capacity.. from classroom


        if return_columns == '*':
            return_cols = [i for i in range(len(self.column_names))]

            column_names=[self.column_names[i] for i in return_cols]
            all_in_on['*'].append((0, return_cols))

        else:
            return_cols=[]
            column_names=[]#Xrhsimeyei oytws wste na ektypwnontai onomata opws to sum(..) poy den yparxoun sthn lista
            index=0 #Exei ton rolo na eisagei tis sthles sto all_in_on opws emfanizontai sto query

            for co in return_columns.split(','):
                if co.lower().startswith('count ('):

                    all_in_on['count'].append((index,self.column_names.index(co[7:-2].strip())))
                    return_cols.append(self.column_names.index(co[7:-2].strip()))
                    column_names.append(co.replace(" ",""))

                elif co.lower().startswith('avg ('):
                    all_in_on['avg'].append((index,self.column_names.index(co[5:-2].strip())))
                    return_cols.append(self.column_names.index(co[5:-2].strip()))
                    column_names.append(co.replace(" ", ""))
                elif co.lower().startswith('min ('):
                    all_in_on['min'].append((index, self.column_names.index(co[5:-2].strip())))
                    return_cols.append(self.column_names.index(co[5:-2].strip()))
                    column_names.append(co.replace(" ",""))
                elif co.lower().startswith('max ('):
                    all_in_on['max'].append((index,self.column_names.index(co[5:-2].strip())))
                    return_cols.append(self.column_names.index(co[5:-2].strip()))
                    column_names.append(co.replace(" ",""))
                elif co.lower().startswith('sum ('):
                    all_in_on['sum'].append((index, self.column_names.index(co[5:-2].strip())))
                    return_cols.append(self.column_names.index(co[5:-2].strip()))
                    column_names.append(co.replace(" ",""))
                elif co.strip()=="*":

                    temp_list=[i for i in range(len(self.column_names))]
                    return_cols.extend(temp_list)
                    column_names.append([self.column_names[i] for i in range(len(self.column_names))])

                    all_in_on['*'].append((index, temp_list))
                else:
                    all_in_on['None'].append((index,self.column_names.index(co.strip())))
                    return_cols.append(self.column_names.index(co.strip()))
                    column_names.append(co.strip())

                index+=1



            # return_cols = [self.column_names.index(col.strip()) for col in return_columns.split(',')]




        # if condition is None, return all rows
        # if not ,condition is splitted and stored in a list named cond and after call function multiple_where_clause
        if condition:

            cond=condition.split()
            rows=self.multiple_where_clause(cond)

        else:
            rows = [i for i in range(len(self.data))]





        # top k rows
        # rows = rows[:int(top_k)] if isinstance(top_k,str) else rows
        # copy the old dict, but only the rows and columns of data with index in rows/columns (the indexes that we want returned)
        dict = {(key):([[self.data[i][j] for j in return_cols] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}




        #Ean to orisma group by den einia keno kaleitai h synarthsh _group_by_method
        if group_by :


            dict['data']=self._group_by_method(group_by,rows,all_in_on,having)

        #Diaforetika ean den exoume group by alla exoume kapoia aggregate function opws to count px tote kaleitai h aggregate
        elif all_in_on['count'] or all_in_on['avg'] or all_in_on['sum'] or all_in_on['min'] or all_in_on['max']:
            dict['data'] = self.aggregate(all_in_on, rows, all_columns)




        # we need to set the new column names/types and no of columns, since we might


        column_names=self.unpacking(column_names)
        dict['column_names'] = column_names
        #[self.column_names[i] for i in return_cols]


        dict['column_types']   = [self.column_types[i] for i in return_cols]


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

