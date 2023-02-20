from __future__ import annotations
from tabulate import tabulate
import pickle
import os
import sys
sys.path.append(f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/miniDB')
from misc import get_op, split_condition , reverse_op


class Table:
    def __init__(self, name=None, column_names=None, column_types=None, primary_key=None, load=None):
        if load is not None:
            # if load is a dict, replace the object dict with it.
            if isinstance(load, dict):
                self.__dict__.update(load)
                # self._update()
            # if load is str, load from a file
            elif isinstance(load, str):
                self.fortose_apo_arxeio(load)

        
        elif (name is not None) and (column_names is not None) and (column_types is not None):
              self._name = name
              if len(column_names)!=len(column_types):
                raise ValueError('Apaiteitai idios arithmos grammon kai stilon.')
              self.column_names = column_names
              self.columns = []
              for col in self.column_names:
                  if col not in self.__dir__():
                     # this is used in order to be able to call a column using its name as an attribute.
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
            

    # if any of the name, columns_names and column types are none. return an empty table object

    def column_by_name(self, onoma_stilis):
        return [row[self.column_names.index(onoma_stilis)] for row in self.data]


    def _update(self): #Updates the columns with the appended rows.
        self.columns = [[row[i] for row in self.data] for i in range(len(self.column_names))]
        for ind, col in enumerate(self.column_names):
            setattr(self, col, self.columns[ind])

    def _cast_column(self, column_name, cast_type):
        '''
        Xrhsima:
            column_name: string. The column that will be casted.
            cast_type: type. Cast type (do not encapsulate in quotes).
        '''
        # get the column from its name
        index_stilis = self.column_names.index(column_name)
        # for every column's value in each row, replace it with itself but casted as the specified type
        for i in range(len(self.data)):
            self.data[i][index_stilis] = cast_type(self.data[i][index_stilis])
        # change the type of the column
        self.column_types[index_stilis] = cast_type
        

    def _insert(self, seira, stoiva=[]):
        if len(seira)!=len(self.column_names):
            raise ValueError(f'Den ginetai na eisaxthoun {len(seira)} times. Yparxoun {len(self.column_names)} sthles')
        for i in range(len(seira)):
            # for each value, cast and replace it in row.
            try:
                seira[i] = self.column_types[i](seira[i])
            except ValueError:
                if seira[i] != 'NULL':
                    raise ValueError(f'I timi {seira[i]} typou {type(seira[i])} den einai ston sosto typo {self.column_types[i]}.')
            except TypeError as eksairesi:
                if seira[i] != None:
                    print(eksairesi)
            # if value is to be appended to the primary_key column, check that it doesnt alrady exist (no duplicate primary keys)
            if i==self.pk_idx and seira[i] in self.column_by_name(self.pk):
                raise ValueError(f'## {seira[i]} yparxei hdh sthn sthlh tou proteuontos kleidiou.')
            elif i==self.pk_idx and seira[i] is None:
                raise ValueError(f'Prepei na exei timi to proteuon kleidi.')

        # if insert_stack is not empty, append to its last index
        if stoiva != []:
            self.data[stoiva[-1]] = seira
        else: 
            self.data.append(seira)
        

    def tropopoisi_seiras(self, orise_timi, orise_stili, synthiki):
        column_name, telestis, value = self.analysi_synthikis(synthiki) #Analysi tis synthikis(parsing)
        # get the condition and the set column
        column = self.column_by_name(column_name)
        set_column_index = self.column_names.index(orise_stili)

        # for each value in column, if condition, replace it with set_value
        for row_ind, column_value in enumerate(column):
            if get_op(telestis, column_value, value):
                self.data[row_ind][set_column_index] = orise_timi

        
    def diagrafi_opou(self, synthiki):
        '''
        Xrhsima:
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.
                Operators supported: (<,<=,==,>=,>)
        '''
        column_name, telestis, value = self.analysi_synthikis(synthiki)
        indexes_pros_diagrafi = []
        stili = self.column_by_name(column_name)
        for index, row_value in enumerate(stili):
            if get_op(telestis, row_value, value):
                indexes_pros_diagrafi.append(index)
        
        for index in sorted(indexes_pros_diagrafi, reverse=True):
            if self._name[:4] != 'meta':
                self.data[index] = [None for _ in range(len(self.column_names))]
            else:
                self.data.pop(index)
        return indexes_pros_diagrafi


    def _select_where(self, emfanisi_stilwn, condition=None, distinct=False, order_by=None, desc=True, orion=None): #To gnosto select...from...where
        '''
        Xrhsima:
            return_columns: list. The columns to be returned.
            condition: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column' or
                'Between , NOT, AND , OR'.
            distinct: boolean. If True, the resulting table will contain only unique rows (False by default).
            order_by: string. A column name that signals that the resulting table should be ordered based on it (no order if None).
            desc: boolean. If True, order_by will return results in descending order.
            orion: int. An integer that defines the number of rows that will be returned (all rows if None).
        '''
        if emfanisi_stilwn == '*': #Returns all columns
            return_cols = [i for i in range(len(self.column_names))]
        else:
            return_cols = [self.column_names.index(col.strip()) for col in emfanisi_stilwn.split(',')]
        if condition is not None:
            if "BETWEEN" in condition.split() or "between" in condition.split():
                split_con = condition.split()
                if (split_con[3] != 'and'):  
                    print('Prepei na exei and metaksy ton arithmon.')
                    exit()
                else:   
                    aristeri_timi = split_con[2]  
                    dexia_timi = split_con[4]  
                    column_name = split_con[0]
                    column = self.column_by_name(column_name)
                    rows = []
                    if (aristeri_timi.isdigit() and dexia_timi.isdigit()):  
                        for i, j in enumerate(column):
                            if int(j) >= int(aristeri_timi) and int(j) <= int(dexia_timi):
                                rows.append(i)  #if between applies
                    else:  
                        print('Den sygkrinontai ta alpharithmitika') #No value other than number accepted
                        exit()
            elif "OR" in condition.split() or "or" in condition.split(): #OR operator
                condition_list = condition.split("OR")
                condition_list = condition_list[0].split("or")
                row_lists = []
                for cond in condition_list: # run every condition seperatly
                    column_name, telestis, value = self.analysi_synthikis(cond)
                    column = self.column_by_name(column_name)
                    row_lists.append([ind for ind, x in enumerate(column) if get_op(telestis, x, value)])
                rows = []
                for l in row_lists: # move all rows into one list
                    for row in l:
                        if not(row in rows):
                            rows.append(row)
            elif "AND" in condition.split() or "and" in condition.split(): #AND operator
                condition_list = condition.split("AND")
                condition_list = condition_list[0].split("and")
                row_lists = []
                for cond in condition_list: # run every condition seperatly
                    column_name, telestis, value = self.analysi_synthikis(cond)
                    column = self.column_by_name(column_name)
                    row_lists.append([ind for ind, x in enumerate(column) if get_op(telestis, x, value)])
                rows = set(row_lists[0]).intersection(*row_lists) # get the intersection of the seperate conditions
            elif "NOT" in condition.split() or "not" in condition.split(): #NOT operator
                condition_list = condition.split("NOT")
                condition_list = condition_list[0].split("not")                 
                column_name, telestis, value = self.analysi_synthikis(condition_list[1])
                column = self.column_by_name(column_name)
                operator2=reverse_op(telestis)
                rows = [ind for ind, x in enumerate(column) if get_op(operator2, x, value)]
            else:
                column_name, telestis, value = self.analysi_synthikis(condition)
                column = self.column_by_name(column_name)                
                rows = [ind for ind, x in enumerate(column) if get_op(telestis, x, value)]
        else: #If condition is None
            rows = [i for i in range(len(self.data))]

        # copy the old dict, but only the rows and columns of data with index
        dict = {(key):([[self.data[i][j] for j in return_cols] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}
        dict['column_names'] = [self.column_names[i] for i in return_cols]
        dict['column_types']   = [self.column_types[i] for i in return_cols]
        pros_emfanisi = Table(load=dict)
        pros_emfanisi.data = list(set(map(lambda x: tuple(x), pros_emfanisi.data))) if distinct else pros_emfanisi.data
        if order_by:
            pros_emfanisi.order_by(order_by, desc)
        if isinstance(orion,str):
            pros_emfanisi.data = [row for row in pros_emfanisi.data if any(row)][:int(orion)]
        return pros_emfanisi


    def epilogi_alla_me_btree(self, return_columns, b_dendro, synthiki, distinct=False, order_by=None, desc=True, orion=None): #Eurethrio b-dendro
        if return_columns == '*':
            emfanisi_stilwn = [i for i in range(len(self.column_names))]
        else:
            emfanisi_stilwn = [self.column_names.index(colname) for colname in return_columns]
        column_name, telestis, value = self.analysi_synthikis(synthiki)
        column = self.column_by_name(column_name)

        # sequential
        rows1 = []
        operations_sequential = 0
        for ind, x in enumerate(column):
            operations_sequential+=1
            if get_op(telestis, x, value):
                rows1.append(ind)

        rows = b_dendro.find(telestis, value)
        try:
            arithmos_oriou = int(orion)
        except TypeError:
            arithmos_oriou = None
        rows = rows[:arithmos_oriou]
        dict = {(key):([[self.data[i][j] for j in emfanisi_stilwn] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}
        dict['column_names'] = [self.column_names[i] for i in emfanisi_stilwn]
        dict['column_types']   = [self.column_types[i] for i in emfanisi_stilwn]
        pros_emfanisi = Table(load=dict)
        pros_emfanisi.data = list(set(map(lambda x: tuple(x), pros_emfanisi.data))) if distinct else pros_emfanisi.data
        if order_by:
            pros_emfanisi.order_by(order_by, desc)
        if isinstance(orion,str):
            pros_emfanisi.data = [row for row in pros_emfanisi.data if row is not None][:int(orion)]
        return pros_emfanisi


    def epilogi_alla_me_hash(self, return_columns, hm, synthiki, distinct=False, order_by=None, desc=True, orion=None):  #I periptosi tou hash
        if return_columns == '*':
            emfanisi_stilwn = [i for i in range(len(self.column_names))]
        else:
            emfanisi_stilwn = [self.column_names.index(colname) for colname in return_columns]
        column_name, telestis, value = self.analysi_synthikis(synthiki)
        rows = []
        if (telestis == '<' or telestis == '>'):
            column = self.column_by_name(column_name)
            # sequential
            operations_sequential = 0
            for ind, x in enumerate(column):
                operations_sequential+=1
                if get_op(telestis, x, value):
                    rows.append(ind)
        else:
            # hash value
            athroisma_hash = 0
            for letter in value:
                athroisma_hash += ord(letter)
            hash_index = athroisma_hash % int(hm[0][0][0])
            # find in dictionary with hash_index
            for item in hm[hash_index]:
                if hm[hash_index][item][0] == hm[0][0][0]:
                    continue
                if hm[hash_index][item][1] == value:
                    rows.append(hm[hash_index][item][0])
            
        try:
            arithmos_oriou = int(orion)
        except TypeError:
            arithmos_oriou = None
        rows = rows[:arithmos_oriou]
        dict = {(key):([[self.data[i][j] for j in emfanisi_stilwn] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}
        dict['column_names'] = [self.column_names[i] for i in emfanisi_stilwn]
        dict['column_types']   = [self.column_types[i] for i in emfanisi_stilwn]
        pros_emfanisi = Table(load=dict)
        pros_emfanisi.data = list(set(map(lambda x: tuple(x), pros_emfanisi.data))) if distinct else pros_emfanisi.data
        if order_by:
            pros_emfanisi.order_by(order_by, desc)
        if isinstance(orion,str):
            pros_emfanisi.data = [row for row in pros_emfanisi.data if row is not None][:int(orion)]
        return pros_emfanisi

    
    def order_by(self, onoma_stilis, desc=True):
        stili = self.column_by_name(onoma_stilis)
        idx = sorted(range(len(stili)), key=lambda k: stili[k], reverse=desc)
        self.data = [self.data[i] for i in idx]
        
    def _general_join_processing(self, table_right:Table, synthiki, join_type):
        # get columns and operator
        column_name_left, telestis, column_name_right = self.analysi_synthikis(synthiki, join=True)
        # try to find both columns, if you fail raise error
        if(telestis != '=' and join_type in ['left','right','full']):
            class CustomFailException(Exception):
                pass
            raise CustomFailException('Den ginetai outer join xoris "=". ')

        try:
            index_stilis_aristerou_pinaka = self.column_names.index(column_name_left)
        except:
            raise Exception(f'H sthlh "{column_name_left}" den yparxei ston aristero pinaka.')

        try:
            index_stilis_dexiou_pinaka = table_right.column_names.index(column_name_right)
        except:
            raise Exception(f'H sthlh "{column_name_right}" den yparxei ston dexio pinaka.')
        # get the column names of both tables with the table name in front
        onomata_stilon_aristerou_pinaka = [f'{self._name}.{colname}' if self._name!='' else colname for colname in self.column_names]
        onomata_stilon_dexiou_pinaka = [f'{table_right._name}.{colname}' if table_right._name!='' else colname for colname in table_right.column_names]
        # define the new table
        onoma_enomenou_pinaka = ''
        stiles_enomenou_pinaka = onomata_stilon_aristerou_pinaka+onomata_stilon_dexiou_pinaka
        typoi_stilon_enomenou_pinaka = self.column_types+table_right.column_types
        join_table = Table(name=onoma_enomenou_pinaka, column_names=stiles_enomenou_pinaka, column_types= typoi_stilon_enomenou_pinaka)
        return join_table, index_stilis_aristerou_pinaka, index_stilis_dexiou_pinaka, telestis


    def _inner_join(self, table_right: Table, condition): #Inner join depending on the condition.
        join_table, index_stilis_aristerou_pinaka, index_stilis_dexiou_pinaka, telestis = self._general_join_processing(table_right, condition, 'inner')
        # count the number of operations (<,> etc)
        metritis_teleston = 0
        for row_left in self.data:
            aristeri_timi = row_left[index_stilis_aristerou_pinakat]
            for row_right in table_right.data:
                dexia_timi = row_right[index_stilis_dexiou_pinaka]
                if(aristeri_timi is None and dexia_timi is None):
                    continue
                metritis_teleston+=1 #Add the row to the table
                if get_op(telestis, aristeri_timi, dexia_timi): 
                    join_table._insert(row_left+row_right)
        return join_table
    
    def enosi_apo_aristera(self, table_right: Table, condition): #Performs left join of the table.
        join_table, index_stilis_aristerou_pinaka, index_stilis_dexiou_pinaka, operator = self._general_join_processing(table_right, condition, 'left')
        dexia_stili = table_right.column_by_name(table_right.column_names[index_stilis_dexiou_pinaka])
        mhkos_grammhs_dexiou_pinaka = len(table_right.column_names)
        for row_left in self.data:
            aristeri_timi = row_left[index_stilis_aristerou_pinaka]
            if aristeri_timi is None:
                continue
            elif aristeri_timi not in dexia_stili:
                join_table._insert(row_left + mhkos_grammhs_dexiou_pinaka*["NULL"])
            else:
                for row_right in table_right.data:
                    dexia_timi = row_right[index_stilis_dexiou_pinaka]
                    if aristeri_timi == dexia_timi:
                        join_table._insert(row_left + row_right)
        return join_table

    def enosi_apo_dexia(self, table_right: Table, condition): #Performs right join of the table.
        join_table, index_stilis_aristerou_pinaka, index_stilis_dexiou_pinaka, operator = self._general_join_processing(table_right, condition, 'right')
        aristeri_stili = self.column_by_name(self.column_names[index_stilis_aristerou_pinaka])
        mhkos_grammhs_aristerou_pinaka = len(self.column_names)
        for row_right in table_right.data:
            dexia_timi = row_right[index_stilis_dexiou_pinaka]
            if dexia_timi is None:
                continue
            elif dexia_timi not in aristeri_stili:
                join_table._insert(mhkos_grammhs_aristerou_pinaka*["NULL"] + row_right)
            else:
                for row_left in self.data:
                    aristeri_timi = row_left[index_stilis_aristerou_pinaka]
                    if aristeri_timi == dexia_timi:
                        join_table._insert(row_left + row_right)
        return join_table
    
    def pliris_enosi(self, table_right: Table, condition): #Performs full join of the table,according to a specific condition.
        join_table, index_stilis_aristerou_pinaka, index_stilis_dexiou_pinaka, telestis = self._general_join_processing(table_right, condition, 'full')
        dexia_stili = table_right.column_by_name(table_right.column_names[index_stilis_dexiou_pinaka])
        aristeri_stili = self.column_by_name(self.column_names[index_stilis_aristerou_pinaka])
        mhkos_grammhs_dexiou_pinaka = len(table_right.column_names)
        mhkos_grammhs_aristerou_pinaka = len(self.column_names)
        for row_left in self.data:
            aristeri_timi = row_left[index_stilis_aristerou_pinaka]
            if aristeri_timi is None:
                continue
            if aristeri_timi not in dexia_stili:
                join_table._insert(row_left + mhkos_grammhs_dexiou_pinaka*["NULL"])
            else:
                for row_right in table_right.data:
                    dexia_timi = row_right[index_stilis_dexiou_pinaka]
                    if aristeri_timi == dexia_timi:
                        join_table._insert(row_left + row_right)
        for row_right in table_right.data:
            dexia_timi = row_right[index_stilis_dexiou_pinaka]
            if dexia_timi is None:
                continue
            elif dexia_timi not in aristeri_stili:
                join_table._insert(mhkos_grammhs_aristerou_pinaka*["NULL"] + row_right)
        return join_table

    def show(self, plithos_grammon=None, einai_kleidomeno=False): #Show the table!
        pros_emfanisi = ""
        if einai_kleidomeno:
            pros_emfanisi += f"\n## {self._name} kleidomeno ##\n"
        else:
            pros_emfanisi += f"\n## {self._name} ##\n"
        # headers -> "column name (column type)"
        headers = [f'{col} ({tp.__name__})' for col, tp in zip(self.column_names, self.column_types)]
        if self.pk_idx is not None:
            # table has a primary key, add PK next to the appropriate column
            headers[self.pk_idx] = headers[self.pk_idx]+' #PK#'
        # detect the rows that are not full of nones
        mh_kenes_seires = [row for row in self.data if any(row)]
        print(tabulate(mh_kenes_seires[:plithos_grammon], headers=headers)+'\n')


    def analysi_synthikis(self, synthiki, join=False):
        '''
        Args:(other than the condition)
            join: boolean. Whether to join or not (False by default).
        '''
        # if the function uses both columns, then return the names of the columns (left first)
        if join:
            return split_condition(synthiki)

        # cast the value with the specified column's type
        aristero, op, dexi = split_condition(synthiki)
        if aristero not in self.column_names:
            raise ValueError(f'Synthiki mh egkurh (den vrisko tin stili pou mou les)')
        typos_sthlhs = self.column_types[self.column_names.index(aristero)]
        return aristero, op, typos_sthlhs(dexi)


    def fortose_apo_arxeio(self, arxeiouonoma): #Loads from a .pkl file 
        arxeio_pros_anoigma = open(arxeiouonoma, 'rb')
        fortoma = pickle.load(arxeio_pros_anoigma)
        arxeio_pros_anoigma.close()
        self.__dict__.update(fortoma.__dict__)
