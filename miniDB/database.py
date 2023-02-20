from __future__ import annotations
import pickle
from time import sleep, localtime, strftime
import os, sys
import logging
import warnings
import readline
from tabulate import tabulate
sys.path.append(f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/miniDB') #Our path for the miniDB
from miniDB import table
sys.modules['table'] = table
from joins import Inlj, Smj
from btree import Btree #Needed for the 2nd question
from misc import split_condition
from table import Table

class Database:
    
    def __init__(self, name, load=True, verbose=True):
        self.tables = {}
        self._name = name
        self.verbose = verbose
        self.savedir = f'dbdata/{name}_db'
        if load:
            try:
                self.fortoma_vasis()
                logging.info(f'Fortonei "{name}".')
                return
            except:
                if verbose:
                    warnings.warn(f'H vash dedomenon "{name}" den yfistatai. Dhmiourgia neas.') #Creates a new database 

        # create dbdata directory if it doesnt exist
        if not os.path.exists('dbdata'):
            os.mkdir('dbdata')
        # create new dbs save directory
        try:
            os.mkdir(self.savedir)
        except:
            pass
        
        self.create_table('meta_length', 'table_name,no_of_rows', 'str,int')
        self.create_table('meta_locks', 'table_name,pid,mode', 'str,int,str')
        self.create_table('meta_insert_stack', 'table_name,indexes', 'str,list')
        self.create_table('meta_indexes', 'table_name,index_name,column_name', 'str,str,str')#After typing the command on Git Bash, meta are firstly created and then the others. 
        self.sosimo_vasis()

    def sosimo_vasis(self): #Swzei thn vash ws arxeio
        for name, table in self.tables.items():
            with open(f'{self.savedir}/{name}.pkl', 'wb') as f:
                pickle.dump(table, f)

    def sosimo_louketon(self): #Swzontai ta locks ws arxeio
        with open(f'{self.savedir}/meta_locks.pkl', 'wb') as f:
            pickle.dump(self.tables['meta_locks'], f)

    def fortoma_vasis(self): 
        path = f'dbdata/{self._name}_db'
        for file in os.listdir(path):
            if file[-3:] != 'pkl':  
                continue
            f = open(path + '/' + file, 'rb')
            tmp_dict = pickle.load(f)
            f.close()
            name = f'{file.split(".")[0]}'
            self.tables.update({name: tmp_dict})
            # setattr(self, name, self.tables[name])

    def _update(self): #Tropopoihsi ton meta-pinakon.
        self.tropopoisi_meta_length()
        self.tropopoisi_tis_stoivas_meta_insert()

    def create_table(self, name, column_names, column_types, primary_key=None, load=None): #Creating and configuring the table
        '''
        This table is saved and can be accessed via db_object.tables['table_name'] or db_object.table_name
        Args:
            name: string. Name of table.
            column_names: list. 
            column_types: list.
            primary_key: string.(if it exists)
            load: boolean. Defines table object parameters as the name of the table and the column names.
        '''
        self.tables.update({name: Table(name=name, column_names=column_names.split(','),
                                        column_types=column_types.split(','), primary_key=primary_key, load=load)})
        # self._name = Table(name=name, column_names=column_names, column_types=column_types, load=load)
        # check that new dynamic var doesnt exist already
        # self.no_of_tables += 1
        self._update()
        self.sosimo_vasis()
        if self.verbose:
            print(f'Dimiourgithike o pinakas "{name}".')

    def diagrafi_pinaka(self, onoma_pinaka):#Delete the table from the db
        self.fortoma_vasis()
        self.kleidoma_pinaka(onoma_pinaka)
        self.tables.pop(onoma_pinaka)
        if os.path.isfile(f'{self.savedir}/{onoma_pinaka}.pkl'):
            os.remove(f'{self.savedir}/{onoma_pinaka}.pkl')
        else:
            warnings.warn(f'"{self.savedir}/{onoma_pinaka}.pkl" den vrethike.')
        self.diagrafi_apo('meta_locks', f'table_name={onoma_pinaka}')
        self.diagrafi_apo('meta_length', f'table_name={onoma_pinaka}')
        self.diagrafi_apo('meta_insert_stack', f'table_name={onoma_pinaka}')
        if self._exei_index(onoma_pinaka):
            pros_diagrafi = []
            for key, table in enumerate(self.tables['meta_indexes'].column_by_name('table_name')):
                if table == onoma_pinaka:
                    pros_diagrafi.append(key)
            for i in reversed(pros_diagrafi):
                self.drop_index(self.tables['meta_indexes'].data[i][1])
        try:
            delattr(self, onoma_pinaka)
        except AttributeError:
            pass
        self.sosimo_vasis()

    def eisagogi_pinaka(self, onoma_pinaka, onoma_arxeiou, column_types=None, primary_key=None): #Create table according to elements of a CSV file.
        arxeio = open(onoma_arxeiou, 'r')
        proti_seira = True
        for line in arxeio.readlines():
            if proti_seira:
                colnames = line.strip('\n')
                if column_types is None:
                    column_types = ",".join(['str' for _ in colnames.split(',')])
                self.create_table(name=onoma_pinaka, column_names=colnames, column_types=column_types, primary_key=primary_key)
                louketo = kleidoma_pinaka(onoma_pinaka, tropos='x')
                proti_seira = False
                continue
            self.tables[onoma_pinaka]._insert(line.strip('\n').split(','))
        if louketo:
            self.xekleidoma_pinaka(onoma_pinaka)
        self._update()
        self.sosimo_vasis()

    def eksagogi_pinaka(self, onoma_pinaka, onoma_arxeiou=None):
        '''
        O pinakas ginetai se morfi arxeiou csv
        '''
        res = ''
        for row in [self.tables[onoma_pinaka].column_names] + self.tables[onoma_pinaka].data:
            res += str(row)[1:-1].replace('\'', '').replace('"', '').replace(' ', '') + '\n'
        if onoma_arxeiou is None:
            onoma_arxeiou = f'{onoma_pinaka}.csv'
        with open(onoma_arxeiou, 'w') as file:
            file.write(res)

    def pinakas_apo_antikeimeno(self, neos_pinakas):
        '''
        Add table object to database.
        '''
        self.tables.update({neos_pinakas._name: neos_pinakas})
        if neos_pinakas._name not in self.__dir__():
            setattr(self, neos_pinakas._name, neos_pinakas)
        else:
            raise Exception(f'"{neos_pinakas._name}" yparxei hdh, vres etero onoma "{self.__class__.__name__}".')
        self._update()
        self.sosimo_vasis()

    #Now, the functions regarding the table.
    def cast(self, stili, onoma_pinaka, cast_type):
        '''
        Modify the type of the specified column and cast all prexisting values.
        (Executes type() for every value in column and saves)
        '''
        self.fortoma_vasis()
        louketo = self.kleidoma_pinaka(onoma_pinaka, tropos='x')
        self.tables[onoma_pinaka]._cast_column(stili, eval(cast_type))
        if louketo:
            self.xekleidoma_pinaka(onoma_pinaka)
        self._update()
        self.sosimo_vasis()

    def insert_into(self, onoma_pinaka, row_str):
        '''
        Args:
            table_name: string.(prepei na apotelei meros tis vaseos).
            row: list. Katalogos timon pros eisagogi (Tha oristei automatos os sygkekrimenos typos).
            lock_load_save: boolean. If False, user needs to load, lock and save the states of the database.
        '''
        row = row_str.strip().split(',')
        self.fortoma_vasis()
        louketo = self.kleidoma_pinaka(onoma_pinaka, tropos='x')
        insert_stack = self.pare_tin_stoiva(onoma_pinaka) #Get the insert stack.
        try:
            self.tables[onoma_pinaka]._insert(row, insert_stack)
        except Exception as eksairesi:
            logging.info(eksairesi)
            logging.info('H DIADIKASIA APETYXE')
        self._update_meta_insert_stack_for_tb(onoma_pinaka, insert_stack[:-1])
        if louketo:
            self.xekleidoma_pinaka(onoma_pinaka)
        self._update()
        self.sosimo_vasis()

    def tropopoisi_pinaka(self, onoma_pinaka, vale_arguments, synthiki): #Allagi se sygkekrimeno simeio tou pinaka
        set_column, set_value = vale_arguments.replace(' ', '').split('=')
        self.fortoma_vasis()
        louketo = self.kleidoma_pinaka(onoma_pinaka, tropos='x')
        self.tables[onoma_pinaka].tropopoisi_seiras(set_value, set_column, condition)
        if louketo:
            self.xekleidoma_pinaka(onoma_pinaka)
        self._update()
        self.sosimo_vasis()

    def diagrafi_apo(self, onoma_pinaka, synthiki): #Diagrafi sykekrimenon simeion tou pinaka.
        self.fortoma_vasis()
        louketo = self.kleidoma_pinaka(onoma_pinaka, tropos='x')
        deleted = self.tables[onoma_pinaka].diagrafi_opou(synthiki)
        if louketo:
            self.xekleidoma_pinaka(onoma_pinaka)
        self._update()
        self.sosimo_vasis()#apothikeusi tis vasis meta apo kathe allagi
        if onoma_pinaka[:4] != 'meta':
            self.valto_sti_stoiva(onoma_pinaka, deleted)
        self.sosimo_vasis()

    def select(self, columns, table_name, condition, distinct=None, order_by=None, limit=True, desc=None, apothikeusi_ws=None, return_object=True): #Deixnei auta poy ikanopoioun thn sunthiki
        '''
        Args:
            all written as parameters, plus the boolean variable distinct.
        '''
        if condition is not None:
            if "BETWEEN" in condition.split() or "between" in condition.split():#Does the condition contain between?
                condition_column = condition.split(" ")[0]
            elif "NOT" in condition.split() or "not" in condition.split():#Looking for condition with NOT operator
                condition_column = condition.split(" ")[0]
            elif "AND" in condition.split() or "and" in condition.split() or "OR" in condition.split() or "or" in condition.split():#Looking for condition containing AND or OR
                condition_column = condition.split(" ")[0]
            else:
                condition_column = split_condition(condition)[0]
        else:
            condition_column = ''
        if self.einai_kleidomeno(table_name): #Is table locked?
            return
        if self._exei_index(table_name) and condition_column == self.tables[table_name].column_names[self.tables[table_name].pk_idx]:
            index_name = \
                self.select('*', 'meta_indexes', f'table_name={table_name}', return_object=True).column_by_name(
                    'index_name')[0]
            b_dendro = self._fortose_index(index_name)
            try:
                table = self.tables[table_name].epilogi_alla_me_btree(columns, b_dendro, condition, distinct, order_by, desc, limit)
            except:
                table = self.tables[table_name].epilogi_alla_me_hash(columns, b_dendro, condition, distinct, order_by, desc, limit) # if btree fails, try hash
        else:
            table = self.tables[table_name]._select_where(columns, condition, distinct, order_by, desc, limit)
        if apothikeusi_ws is not None:
            table._name = apothikeusi_ws
            self.pinakas_apo_antikeimeno(table)
        else:
            if return_object:
                return table
            else:
                return table.show()

    def deixe_ton_pinaka(self, onoma_pinaka, plithos_grammon=None): #Showing the table
        self.fortoma_vasis()
        self.tables[onoma_pinaka].show(plithos_grammon, self.einai_kleidomeno(onoma_pinaka))

    def taxinomisi(self, onoma_pinaka, stili, asc=False):
        '''
        asc: If True sort will return results in ascending order (False by default).
        '''
        self.fortoma_vasis()
        louketo = self.kleidoma_pinaka(onoma_pinaka, tropos='x')
        self.tables[onoma_pinaka]._sort(stili, asc=asc)
        if louketo:
            self.xekleidoma_pinaka(onoma_pinaka)
        self._update()
        self.sosimo_vasis()

    def provoli(self, table_name, table): #Emfanisi provolis tou pinaka
        table._name = table_name
        self.pinakas_apo_antikeimeno(table)

    def enosi_opou_sinthiki_isxyei(self, tropos, left_table, right_table, synthiki, apothikeusi_ws=None, emfanisi_antikeimenou=True):
        '''
        Args:
            left_table: string. Name of the left table (must be in DB) or Table obj.
            right_table: string. Name of the right table (must be in DB) or Table obj.
            synthiki: string. A condition using the following format:
                'column[<,<=,==,>=,>]value' or
                'value[<,<=,==,>=,>]column'.
                Operators supported: (<,<=,==,>=,>)
        save_as: string. The output filename that will be used to save the resulting table in the database (won't save if None).
        emfanisi_antikeimenou: boolean. If True, the result will be a table object (useful for internal usage - the result will be printed by default).
        '''
        self.fortoma_vasis()
        if self.einai_kleidomeno(left_table) or self.einai_kleidomeno(right_table):
            return
        left_table = left_table if isinstance(left_table, Table) else self.tables[left_table]
        right_table = right_table if isinstance(right_table, Table) else self.tables[right_table]
        if tropos == 'inner':#Inner join
            enosi = left_table._inner_join(right_table, synthiki)
        elif tropos == 'left':
            enosi = left_table.enosi_apo_aristera(right_table, synthiki) #Left join 
        elif tropos == 'right':
            enosi = left_table.enosi_apo_dexia(right_table, synthiki) #Right join
        elif tropos == 'full':
            enosi = left_table.pliris_enosi(right_table, synthiki) #Full join
        elif tropos == 'inl': #for index-nested-loop join
            # Check if there is an index of either of the two tables available.
            yparxei_aristero_index = self._exei_index(left_table._name)
            yparxei_dexio_index = self._exei_index(right_table._name)
            if not yparxei_aristero_index and not yparxei_dexio_index:
                enosi = None
                raise Exception('Adynati i ektelesi tou INLJ. Kalytera kane inner join.')
            elif yparxei_dexio_index:
                index_name = \
                    self.select('*', 'meta_indexes', f'table_name={right_table._name}',
                                emfanisi_antikeimenou=True).column_by_name(
                        'index_name')[0]
                enosi = Inlj(synthiki, left_table, right_table, self._fortose_index(index_name), 'right').enosi_opou_sinthiki_isxyei()
            elif yparxei_aristero_index:
                index_name = \
                    self.select('*', 'meta_indexes', f'table_name={left_table._name}',
                                emfanisi_antikeimenou=True).column_by_name(
                        'index_name')[0]
                enosi = Inlj(synthiki, left_table, right_table, self._fortose_index(index_name), 'left').enosi_opou_sinthiki_isxyei()
        elif tropos == 'sm':
            enosi = Smj(synthiki, left_table, right_table).enosi_opou_sinthiki_isxyei()#To implement sort-merge join
        else:
            raise NotImplementedError
        if apothikeusi_ws is not None:
            enosi._name = apothikeusi_ws
            self.pinakas_apo_antikeimeno(res)
        else:
            if emfanisi_antikeimenou:
                return enosi
            else:
                enosi.show()
        if emfanisi_antikeimenou:
            return enosi
        else:
            enosi.show()

    def kleidoma_pinaka(self, onoma_pinaka, tropos='x'): #Lock the table!
        if onoma_pinaka[:4] == 'meta' or onoma_pinaka not in self.tables.keys() or isinstance(onoma_pinaka, Table):
            return
        with open(f'{self.savedir}/meta_locks.pkl', 'rb') as f:
            self.tables.update({'meta_locks': pickle.load(f)})
        try:
            pid = self.tables['meta_locks']._select_where('pid', f'table_name={onoma_pinaka}').data[0][0]
            if pid != os.getpid():
                raise Exception(f'O pinakas "{onoma_pinaka}" einai kleidomenos apo thn diadikasia me pid={pid}')
            else:
                return False
        except IndexError:
            pass
        if tropos == 'x':
            self.tables['meta_locks']._insert([onoma_pinaka, os.getpid(), tropos])
        else:
            raise NotImplementedError
        self.sosimo_louketon()
        return True
        

    def xekleidoma_pinaka(self, onoma_pinaka, isxys=False): #Unlock the table.
        if onoma_pinaka not in self.tables.keys():
            raise Exception(f'O pinakas "{onoma_pinaka}" den einai stin vasi')
        if not isxys:
            try:
                pid = self.tables['meta_locks']._select_where('pid', f'table_name={onoma_pinaka}').data[0][0]
                if pid != os.getpid():
                    raise Exception(f'O pinakas "{onoma_pinaka}" einai kleidomenos apo thn diadikasia me pid={pid}')
            except IndexError:
                pass
        self.tables['meta_locks'].diagrafi_opou(f'table_name={onoma_pinaka}')
        self.sosimo_louketon()
        

    def einai_kleidomeno(self, onoma_pinaka):
        if isinstance(onoma_pinaka, Table) or onoma_pinaka[:4] == 'meta':  # meta tables will never be locked (they are internal)
            return False
        with open(f'{self.savedir}/meta_locks.pkl', 'rb') as f:
            self.tables.update({'meta_locks': pickle.load(f)})
        try:
            pid = self.tables['meta_locks']._select_where('pid', f'table_name={onoma_pinaka}').data[0][0]
            if pid != os.getpid():
                raise Exception(f'O pinakas "{onoma_pinaka}" einai kleidomenos apo tin diadikasia me pid={pid}')
        except IndexError:
            pass
        return False

    #Functions regarding the meta tables
    def tropopoisi_meta_length(self):
        for table in self.tables.values():
            if table._name[:4] == 'meta':  #meta skipped
                continue
            if table._name not in self.tables['meta_length'].column_by_name(
                    'table_name'):  # if new table, add record with 0 no. of rows
                self.tables['meta_length']._insert([table._name, 0])
            
            mh_kenes_seires = len([row for row in table.data if any(row)]) #Mh kenes seires
            self.tables['meta_length'].tropopoisi_seiras(mh_kenes_seires, 'no_of_rows', f'table_name={table._name}')
            

    def tropopoisi_louketwn_meta(self):
        for table in self.tables.values():
            if table._name[:4] == 'meta':  # meta skipped
                continue
            if table._name not in self.tables['meta_locks'].column_by_name('table_name'):
                self.tables['meta_locks']._insert([table._name, False])
                

    def tropopoisi_tis_stoivas_meta_insert(self):
        for table in self.tables.values():
            if table._name[:4] == 'meta':  # meta skipped
                continue
            if table._name not in self.tables['meta_insert_stack'].column_by_name('table_name'):
                self.tables['meta_insert_stack']._insert([table._name, []])

    def valto_sti_stoiva(self, onoma_pinaka, indexes): #Adding indexes to the table's stack.
        old_lst = self.pare_tin_stoiva(onoma_pinaka)
        self._update_meta_insert_stack_for_tb(onoma_pinaka, old_lst + indexes)

    def pare_tin_stoiva(self, table_name):
        return \
            self.tables['meta_insert_stack']._select_where('*', f'table_name={table_name}').column_by_name('indexes')[0]
       

    def _update_meta_insert_stack_for_tb(self, onoma_pinaka, nea_stoiva):
        self.tables['meta_insert_stack'].tropopoisi_seiras(nea_stoiva, 'indexes', f'table_name={onoma_pinaka}')

    # indexes
    def create_index(self, index_name, onoma_pinaka, onoma_stilis, typos_eurethriou): #Index in a specific column of the table
        if onoma_pinaka not in self.tables:   #Non-existing table
            raise Exception('Den einai dynath h dhmiourgia eurethriou se pinaka pou den yparxei.')
        if onoma_stilis not in self.tables[onoma_pinaka].column_names:   #Non-existing column
            raise Exception('Den einai dynath h dhmiourgia eurethriou se sthlh pou den yparxei.')    
        if index_name not in self.tables['meta_indexes'].column_by_name('index_name'):  #if index_name already exists,or not.
            if typos_eurethriou == 'BTREE' or typos_eurethriou == 'btree': #Trying to insert Btree index on the meta_indexes table
                logging.info('Ftiaxno to btree.')
                self.tables['meta_indexes']._insert([onoma_pinaka, index_name, onoma_stilis])
                self.kataskeui_euretiriou(onoma_pinaka, index_name, onoma_stilis)
                self.sosimo_vasis()
            elif typos_eurethriou == 'HASH' or typos_eurethriou == 'hash': #Trying to insert Hash index on the meta_indexes table
                logging.info('Ftiaxno to hash.')
                self.tables['meta_indexes']._insert([onoma_pinaka, index_name, onoma_stilis])
                self.ftiaxe_hash_euretirio(onoma_pinaka, index_name, onoma_stilis)
                self.sosimo_vasis()
        else:
            raise Exception('Hdh yparxei eurethrio me auto to onoma.')

    def kataskeui_euretiriou(self, onoma_pinaka, index_name, onoma_stilis):  #Create Btree in the primary_key of the table specified
        b_dendro = Btree(3)  
        for idx, key in enumerate(self.tables[onoma_pinaka].column_by_name(onoma_stilis)):
            if key is None:
                continue
            b_dendro.insert(key, idx) 
        self._apothikeusi_index(index_name, b_dendro)
        print('To eftiaxa to btree eurethrio.')
        return

    def ftiaxe_hash_euretirio(self, onoma_pinaka, index_name, onoma_stilis):
        mhkos_grammhs = len(self.tables[onoma_pinaka].data)
        hm = {}
        hm[0] = {}
        hm[0][0] = [str(mhkos_grammhs)] # store the number of the rows
        for idx, key in enumerate(self.tables[onoma_pinaka].column_by_name(onoma_stilis)):
                if key is None:
                    continue
                hash_athroisma = 0
                for letter in key:
                    hash_athroisma += ord(letter) #To gramma to metatrepoume ston antistoixo arithmo tou, px gia to a einai 1, gia to b einai 2 ktl.
                index_megalou_hash = hash_athroisma % mhkos_grammhs #modulo 
                index_mikroterou_hash = index_megalou_hash
                if not(index_megalou_hash in hm):
                    hm[index_megalou_hash] = {}
                else:
                    while True:
                        if not(index_mikroterou_hash in hm[index_megalou_hash]):
                            break
                        if (index_mikroterou_hash == mhkos_grammhs):
                            index_mikroterou_hash = 0
                        else:
                            index_mikroterou_hash += 1
                hm[index_megalou_hash][index_mikroterou_hash] = [idx, key]
        self._apothikeusi_index(index_name, hm)
        print ('To eftiaxa to hash eurethrio.')

    def _exei_index(self, onoma_pinaka): #Does the column have index?
        return onoma_pinaka in self.tables['meta_indexes'].column_by_name('table_name')

    def _apothikeusi_index(self, index_name, index): #Save the index!
        try:
            os.mkdir(f'{self.savedir}/indexes')
        except:
            pass
        with open(f'{self.savedir}/indexes/meta_{index_name}_index.pkl', 'wb') as f:
            pickle.dump(index, f)

    def _fortose_index(self, index_name):#Load index.
        arxeion = open(f'{self.savedir}/indexes/meta_{index_name}_index.pkl', 'rb')
        eurethrio = pickle.load(arxeion)
        arxeion.close()
        return eurethrio

    def drop_index(self, index_name): #Get rid of the index.
        if index_name in self.tables['meta_indexes'].column_by_name('index_name'):
            self.diagrafi_apo('meta_indexes', f'index_name = {index_name}')
            if os.path.isfile(f'{self.savedir}/indexes/meta_{index_name}_index.pkl'):
                os.remove(f'{self.savedir}/indexes/meta_{index_name}_index.pkl')
            else:
                warnings.warn(f'"{self.savedir}/indexes/meta_{index_name}_index.pkl" den vrethike.')
            self.sosimo_vasis()
            print('Entaxei, to diegrapsa.')
        else:
            raise Exception('Den yparxei eurethrio me auto to onoma.')   
