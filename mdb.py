import os
import re
from pprint import pprint
import sys
import readline
import traceback
import shutil
import pickle
import pandas as panda 
sys.path.append('miniDB')

from miniDB.database import Database
from miniDB.table import Table

from miniDB.database import Database
from miniDB.table import Table

from tabulate import tabulate
# art font is "big"
art = '''
             _         _  _____   ____  
            (_)       (_)|  __ \ |  _ \     
  _ __ ___   _  _ __   _ | |  | || |_) |
 | '_ ` _ \ | || '_ \ | || |  | ||  _ < 
 | | | | | || || | | || || |__| || |_) |
 |_| |_| |_||_||_| |_||_||_____/ |____/   2023                            
'''   


def search_between(s, first, last):
    '''
    Search in 's' for the substring that is between 'first' and 'last'
    '''
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
    except:
        return
    return s[start:end].strip()

def in_paren(qsplit, ind):
    '''
    Split string on space and return whether the item in index 'ind' is inside a parentheses
    '''
    return qsplit[:ind].count('(')>qsplit[:ind].count(')')


def create_query_plan(query, keywords, action):
    #print('2.0 Create QueryPlan Start')
    #print("2.1 query: ",query)
    #print("2.1.1 keywords, should be identical to 1.2: ",keywords)
    '''
    Given a query, the set of keywords that we expect to pe present and the overall action, return the query plan for this query.

    This can and will be used recursively
    '''

    dic = {val: None for val in keywords if val!=';'}
    #print("2.2 dic: ",dic) # crafts the dictionary template with none values for each key

    ql = [val for val in query.split(' ') if val !=''] # ql stands for querylist, its the query, but in list form
    #print("2.3 ql: ",ql)

    kw_in_query = []
    kw_positions = []
    i=0
    while i<len(ql):
        if in_paren(ql, i): 
            i+=1
            continue
        if ql[i] in keywords:
            kw_in_query.append(ql[i])
            kw_positions.append(i)
        
        elif i!=len(ql)-1 and f'{ql[i]} {ql[i+1]}' in keywords:
            kw_in_query.append(f'{ql[i]} {ql[i+1]}')
            ql[i] = ql[i]+' '+ql[i+1]
            ql.pop(i+1)
            kw_positions.append(i)
        i+=1
    #print ("kw_in_query: ",kw_in_query)
    #print ("kw_positions: ",kw_positions)
        


    for i in range(len(kw_in_query)-1):
        dic[kw_in_query[i]] = ' '.join(ql[kw_positions[i]+1:kw_positions[i+1]])
        #print ("dic[kw_in_query[i]]: ",dic[kw_in_query[i]])
    #print("dic after loop: ",dic)
    
    if action == 'create view':
        #print('ACTION = CREATE VIEW')
        dic['as'] = interpret(dic['as'])

    if action=='select':
        #print('ACTION = SELECT')
        dic = evaluate_from_clause(dic)

        if dic['distinct'] is not None:
            dic['select'] = dic['distinct']
            dic['distinct'] = True

        if dic['order by'] is not None:
            dic['from'] = dic['from']
            if 'desc' in dic['order by']:
                dic['desc'] = True
            else:
                dic['desc'] = False
            dic['order by'] = dic['order by'].removesuffix(' asc').removesuffix(' desc')
            
        else:
            dic['desc'] = None

    if action=='create table':
        #print('ACTION = CREATE TABLE')

        query_list = query.split()
        table_index = query_list.index("table")
        tab_name = query_list[table_index + 1]
        args = dic['create table'][dic['create table'].index('('):dic['create table'].index(')')+1]
        
        dic['create table'] = dic['create table'].removesuffix(args).strip()
        arg_nopk = args.replace('primary key', '')[1:-1]
        arg_noUnique=args.replace('unique', '')[1:-1]
        arglist = [val.strip().split(' ') for val in arg_nopk.split(',')]
       
        dic['column_names'] = ','.join([val[0] for val in arglist])
        dic['column_types'] = ','.join([val[1] for val in arglist])

        keyboy=0
        if 'primary key' in args:
            arglist = args[1:-1].split(' ')
            dic['primary key'] = arglist[arglist.index('primary')-2]
            keyboy=arglist[arglist.index('primary')-2]
            
        else:
            dic['primary key'] = None

        
        if 'unique' in arg_nopk:
            '''
            Here we check if there are unique arguments in arg nopk and if there is,
            we create pkl fine to add them in , so that we can later use them and change them 
            in case of deletion or to check if there is indeed a unique column when trying to index a 
            unique column using B+Tree indexing
            '''
            split_arg = arg_nopk.split()
            
            row1= split_arg[split_arg.index('unique') - 2]
            
            table1=tab_name
            data = {"tab_name": table1,"primary_key":keyboy, "unique_column": row1}
            #data=row1,'',keyboy

            '''
            Here we start creating the file and checking each case ,
            if there is a file etc
            '''
            if 'unique_table' in locals():
               
                dataFR=unique_table
                
                
                
            elif os.path.isfile('./unique_table.pkl'):
                dataFR=panda.read_pickle('./unique_table.pkl')  

            else:
                dataFR = panda.DataFrame(columns=['tab_name', 'primary_key', 'unique_column'])
                
                

            dataFR=dataFR.append({'tab_name': table1, 'primary_key': keyboy, 'unique_column': row1},ignore_index=True)

            unique_table=dataFR
            dataFR.to_pickle('./unique_table.pkl')           
    
    if action=='import': 
        #print('ACTION = IMPORT')
        dic = {'import table' if key=='import' else key: val for key, val in dic.items()}

    if action=='insert into':
        #print('ACTION = INSERT INTO')
        if dic['values'][0] == '(' and dic['values'][-1] == ')':
            dic['values'] = dic['values'][1:-1]
        else:
            raise ValueError('Your parens are not right m8') #wut?
    
    if action=='unlock table':
        #print('ACTION= UNLOCK TABLE')
        if dic['force'] is not None:
            dic['force'] = True
        else:
            dic['force'] = False

    return dic
def create_and_write_to_file(file_name, content):
    with open(file_name, "w") as file:
        file.write(content)
    print(f"File '{file_name}' created and written to successfully!")

#create_and_write_to_file("new_file.txt", "This is the content written to the file.")



def evaluate_from_clause(dic):
    '''
    Evaluate the part of the query (argument or subquery) that is supplied as the 'from' argument
    '''
    join_types = ['inner', 'left', 'right', 'full', 'sm', 'inl']
    from_split = dic['from'].split(' ')
    if from_split[0] == '(' and from_split[-1] == ')':
        subquery = ' '.join(from_split[1:-1])
        dic['from'] = interpret(subquery)

    join_idx = [i for i,word in enumerate(from_split) if word=='join' and not in_paren(from_split,i)]
    on_idx = [i for i,word in enumerate(from_split) if word=='on' and not in_paren(from_split,i)]
    if join_idx:
        join_idx = join_idx[0]
        on_idx = on_idx[0]
        join_dic = {}
        if from_split[join_idx-1] in join_types:
            join_dic['join'] = from_split[join_idx-1]
            join_dic['left'] = ' '.join(from_split[:join_idx-1])
        else:
            join_dic['join'] = 'inner'
            join_dic['left'] = ' '.join(from_split[:join_idx])
        join_dic['right'] = ' '.join(from_split[join_idx+1:on_idx])
        join_dic['on'] = ''.join(from_split[on_idx+1:])

        if join_dic['left'].startswith('(') and join_dic['left'].endswith(')'):
            join_dic['left'] = interpret(join_dic['left'][1:-1].strip())

        if join_dic['right'].startswith('(') and join_dic['right'].endswith(')'):
            join_dic['right'] = interpret(join_dic['right'][1:-1].strip())

        dic['from'] = join_dic
        
    return dic

def interpret(query):
    #print('1. Interpret Start')
    '''
    Interpret the query.
    '''
    #INTERVENTION FOR B+TREE INDEX ON A UNIQUE COLUMN 
    '''
    The way we are checking and interpreting the query of : 
    create index INDEX_NAME on TABLE_THAT_HAS_A_UNIQUE_COLUMN(UNIQUE_COLUMN_NAME) using btree --->(ONLY B+TREE SUPPORTED)
    is to check if the inserted query is equal to similar1 and then we make an overwrite on the index unique file
    the new overwriten data will be used inside the Database.py file to check before creating an index
    if the new index is create on a unique column or on its PK 
    '''

    similar1 = r"create index (\w+) on (\w+)\((\w+)\) using btree"
    similar_to= re.search(similar1,query)
    ok='ok'
    if similar_to:
        
        index_name=similar_to.group(1)
        table_name=similar_to.group(2)
        table_column=similar_to.group(3)
        print(index_name,"  ",table_name,"  ",table_column)
        if os.path.isfile('./index_uniques.pkl'):
            dataFR5=panda.read_pickle('./index_uniques.pkl')
            dataFR5.loc[0]=table_name,table_column,index_name
            dataFR5.to_pickle('./index_uniques.pkl')

            dataFR5=panda.read_pickle('index_uniques.pkl')
           
        else:
            dataFR5=panda.DataFrame(columns=['table_name','table_column','index_name'])
            dataFR5.loc[0]=table_name,table_column,index_name
            dataFR5.to_pickle('./index_uniques.pkl')
            dataFR5=panda.read_pickle('index_uniques.pkl')
            

        
        


    #keyword per action?
    kw_per_action = {'create table': ['create table'],
                     'drop table': ['drop table'],
                     'cast': ['cast', 'from', 'to'],
                     'import': ['import', 'from'],
                     'export': ['export', 'to'],
                     'insert into': ['insert into', 'values'],
                     'select': ['select', 'from', 'where', 'distinct', 'order by', 'limit'],
                     #'select': ['select', 'from', 'where', 'distinct', 'between', 'and', 'order by', 'limit'],
                     'lock table': ['lock table', 'mode'],
                     'unlock table': ['unlock table', 'force'],
                     'delete from': ['delete from', 'where'],
                     'update table': ['update table', 'set', 'where'],
                     'create index': ['create index', 'on', 'using'],
                     'drop index': ['drop index'],
                     'create view' : ['create view', 'as']
                     } # ta keys einai ta aristera, to eidos.

    if query[-1]!=';': # add ; if it doesnt exist (at the end)
        query+=';'
    
    query = query.replace("(", " ( ").replace(")", " ) ").replace(";", " ;").strip() # vazei kena gia kapio logo deksia-aristera apo tis parenthesis ( kai ), to idio kai gia to ;, vazei ena keno prin, stin periptosi poy kolitike example; ==> example ;

    # for kw in kw_per_action.keys(): #gia kathe key sta keys psakse
    #     if query.startswith(kw): #finds what type of action we have to deal with by checking the first string that exists inside the query
    #         action = kw # example: query string starts with: "select ..." --> action = "select"
    #         print ("1.1 action type: ",action)

    # #print("1.2 kw_per_action[action]+[';'] == ",kw_per_action[action]+[';'])
    # return create_query_plan(query, kw_per_action[action]+[';'], action)
    multipleQueries=False
    word_query=query.split() #splits query into words
    for keyword in word_query: # detects if query has and's/or's
        if(keyword=="or" or keyword=="and"): 
            multipleQueries=True
            logical_operator=keyword

    if(multipleQueries): #if the query contains and's/or's, splits the query into smaller individual ones, then passes them for query planning
        base_query=[]
        for word in word_query: # forming the base query (query start until "where")
            base_query.append(word) 
            if(word=='where'):
                stopIndex = word_query.index("where") # will need the index later
                break   # base query has now been formed        
        tails=[] # each logical operator equals one more independent query to construct
        query_tail=[] # reformed queries = query_base + query tail
        for i in range(stopIndex+1,len(word_query)): # continue right after "where"
            if(word_query[i]=='or' or word_query[i]=='and'):
                query_tail.append(";") # end of query
                tails.append(query_tail) 
                query_tail=[] #if or/and is found save appended tail to list, clear tail and continue with the next one (if any)
            else:
                query_tail.append(word_query[i])
        tails.append(query_tail) #last tail
        #print (*tails)
        formed_queries=[]
        for query_tail in tails: # connect base_query + query_tails to form final independent queries 
            formed_queries.append(base_query+query_tail)
        #print ("reformed QUERY",*formed_queries)
        final_query = []
        for ref_query in formed_queries: # finalizing query form for query planning
            ref_query = ' '.join(ref_query)
            final_query.append(ref_query)
        query_plans=[]
        for query in final_query: # query plans
            for kw in kw_per_action.keys():
                if query.startswith(kw):
                    action = kw
            query_plans.append(create_query_plan(query, kw_per_action[action]+[';'], action))
        return query_plans,logical_operator # each query plan goes hand to hand with a logical operator indication in order to be handled accordingly
    else:
        for kw in kw_per_action.keys():
            if query.startswith(kw):
                action = kw
        return create_query_plan(query, kw_per_action[action]+[';'], action),'none' # regular query = tag for logical operator will be 'none'



def execute_dic(dic):
    '''
    Execute the given dictionary
    '''
    for key in dic.keys():
        if isinstance(dic[key],dict):
            dic[key] = execute_dic(dic[key])
    
    action = list(dic.keys())[0].replace(' ','_')
    return getattr(db, action)(*dic.values())

def interpret_meta(command):
    """
    Interpret meta commands. These commands are used to handle DB stuff, something that can not be easily handled with mSQL given the current architecture.

    The available meta commands are:

    lsdb - list databases
    lstb - list tables
    cdb - change/create database
    rmdb - delete database
    """
    action = command.split(' ')[0].removesuffix(';')

    db_name = db._name if search_between(command, action,';')=='' else search_between(command, action,';')

    verbose = True
    if action == 'cdb' and ' -noverb' in db_name:
        db_name = db_name.replace(' -noverb','')
        verbose = False

    def list_databases(db_name):
        [print(fold.removesuffix('_db')) for fold in os.listdir('dbdata')]
    
    def list_tables(db_name):
        [print(pklf.removesuffix('.pkl')) for pklf in os.listdir(f'dbdata/{db_name}_db') if pklf.endswith('.pkl')\
            and not pklf.startswith('meta')]

    def change_db(db_name):
        global db
        db = Database(db_name, load=True, verbose=verbose)
    
    def remove_db(db_name):
        shutil.rmtree(f'dbdata/{db_name}_db')

    commands_dict = {
        'lsdb': list_databases,
        'lstb': list_tables,
        'cdb': change_db,
        'rmdb': remove_db,
    }

    commands_dict[action](db_name)


if __name__ == "__main__":
    fname = os.getenv('SQL')
    dbname = os.getenv('DB')

    db = Database(dbname, load=True)

    

    if fname is not None:
        for line in open(fname, 'r').read().splitlines():
            if line.startswith('--'): continue
            if line.startswith('explain'):
                dic = interpret(line.removeprefix('explain '))
                pprint(dic, sort_dicts=False)
            else :
                dic = interpret(line.lower())
                result = execute_dic(dic)
                if isinstance(result,Table):
                    result.show()
        

    from prompt_toolkit import PromptSession
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.auto_suggest import AutoSuggestFromHistory

    print(art)
    session = PromptSession(history=FileHistory('.inp_history'))
    while 1:
        try:
            line = session.prompt(f'({db._name})> ', auto_suggest=AutoSuggestFromHistory()).lower()
            if line[-1]!=';':
                line+=';'
        except (KeyboardInterrupt, EOFError):
            print('\nbye!')
            break
        try:
            #print('0. This is program Start')
            if line=='exit': # exit program 
                break
            if line.split(' ')[0].removesuffix(';') in ['lsdb', 'lstb', 'cdb', 'rmdb']: #only for meta commands
                interpret_meta(line)
            elif line.startswith('explain'):
                dic = interpret(line.removeprefix('explain '))
                pprint(dic, sort_dicts=False)
            else: # normal operation goes here
                # print("--- INTERPRET START ----")
                # dic = interpret(line)
                # print("--- EXECUTE START --- ")
                # result = execute_dic(dic)
                # #print("--- TRY SHOW RESULT---")
                # showResult = False
                # if isinstance(result,Table):
                #     showResult = True
                #     print("--- SHOW RESULT SUCCESS ---")
                #     result.show()
                # if (showResult==False):
                #     print("---SHOW RESULT FAIL--- (isinstance(result,Table)==False)")
                results=[]
                result_table=[]
                temp_table = []
                
                allQueryResults = []
                dic,logical_operator = interpret(line)
                #print("SPECIAL",logical_operator,"DIC",dic)
                if(logical_operator=='none'): #no change
                    result = execute_dic(dic)
                    if isinstance(result,Table):
                        result.show()
                elif(logical_operator=='and'): # and operations
                    for query in dic:
                        results.append(execute_dic(query))
                    for r in results:
                        header,qr=r.show(print_output=False)
                        #print("QueryResult:",qr)
                        allQueryResults.append(qr)
                    for qr in allQueryResults:
                        for row in qr:
                            if (row in temp_table):
                                result_table.append(row)
                                #print(row,"(AND) DUPE - OK") # debug
                            else:
                                temp_table.append(row)
                                #print(row,"(AND) Denied") # debug
                    print(tabulate(result_table[:None], headers=header)+'\n')
                elif(logical_operator=='or'): # or operations
                    results=[]
                    result_table=[]
                    allQueryResults = []
                    for query in dic:
                        results.append(execute_dic(query))
                    for r in results:
                        header,qr=r.show(print_output=False)
                        #print("QueryResult:",qr)
                        allQueryResults.append(qr)
                    for qr in allQueryResults:
                        for row in qr:
                            if (row not in result_table):
                                result_table.append(row)
                                #print(row,"(OR) OK") # debug
                            else:
                                #print(row,"(OR) DUPLICATE (Denied)") # debug
                                pass
                    print(tabulate(result_table[:None], headers=header)+'\n')
        except Exception: # errors
            print(traceback.format_exc())
