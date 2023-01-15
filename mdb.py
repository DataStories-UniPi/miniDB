import os
import re
from pprint import pprint
import sys
import readline
import traceback
import shutil
sys.path.append('miniDB')

from database import Database
from table import Table
# art font is "big"
art = '''
             _         _  _____   ____  
            (_)       (_)|  __ \ |  _ \     
  _ __ ___   _  _ __   _ | |  | || |_) |
 | '_ ` _ \ | || '_ \ | || |  | ||  _ < 
 | | | | | || || | | || || |__| || |_) |
 |_| |_| |_||_||_| |_||_||_____/ |____/   2022                              
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
    '''
    Given a query, the set of keywords that we expect to pe present and the overall action, return the query plan for this query.

    This can and will be used recursively
    '''

    dic = {val: None for val in keywords if val!=';'}

    ql = [val for val in query.split(' ') if val !='']

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
        


    for i in range(len(kw_in_query)-1):
        dic[kw_in_query[i]] = ' '.join(ql[kw_positions[i]+1:kw_positions[i+1]])
    
    if action == 'create view':
        #dic['as'] = interpret(dic['as'])
        temp = interpret(dic['as'])
        if (type(temp) is tuple): # Keep only the dictionary
            dic['as'] = temp[0]
        else:
            dic['as'] = temp

    if action=='select':
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
        args = dic['create table'][dic['create table'].index('('):dic['create table'].index(')')+1]
        dic['create table'] = dic['create table'].removesuffix(args).strip()
        arg_nopk = args.replace('primary key', '')[1:-1]
        arglist = [val.strip().split(' ') for val in arg_nopk.split(',')]
        dic['column_names'] = ','.join([val[0] for val in arglist])
        dic['column_types'] = ','.join([val[1] for val in arglist])
        # For columns with UNIQUE constraint
        dic['columns_unique'] = ','.join([val[0] for val in arglist if 'unique' in val]) if 'unique' in args else None
        
        
        if 'primary key' in args:
            arglist = args[1:-1].split(' ')
            dic['primary key'] = arglist[arglist.index('primary')-2]
        else:
            dic['primary key'] = None
        
        
    if action=='import': 
        dic = {'import table' if key=='import' else key: val for key, val in dic.items()}

    if action=='insert into':
        if dic['values'][0] == '(' and dic['values'][-1] == ')':
            dic['values'] = dic['values'][1:-1]
        else:
            raise ValueError('Your parens are not right m8')
    
    if action=='unlock table':
        if dic['force'] is not None:
            dic['force'] = True
        else:
            dic['force'] = False

    return dic



def evaluate_from_clause(dic):
    '''
    Evaluate the part of the query (argument or subquery) that is supplied as the 'from' argument
    '''
    join_types = ['inner', 'left', 'right', 'full', 'sm', 'inl']
    from_split = dic['from'].split(' ')
    if from_split[0] == '(' and from_split[-1] == ')':
        subquery = ' '.join(from_split[1:-1])
        #dic['from'] = interpret(subquery)
        temp = interpret(subquery)
        if (type(temp) is tuple):
            dic['from'] = temp[0]
        else:
            dic['from'] = temp

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
            #join_dic['left'] = interpret(join_dic['left'][1:-1].strip())
            temp = interpret(join_dic['left'][1:-1].strip())
            if (type(temp) is tuple):
                join_dic['left'] = temp[0]
            else:
                join_dic['left'] = temp

        if join_dic['right'].startswith('(') and join_dic['right'].endswith(')'):
            #join_dic['right'] = interpret(join_dic['right'][1:-1].strip())
            temp = interpret(join_dic['right'][1:-1].strip())
            if (type(temp) is tuple):
                join_dic['right'] = temp[0]
            else:
                join_dic['right'] = temp
        dic['from'] = join_dic
        
    return dic

def check_operator(query):
    '''
    Checks the query for the 'and' and 'or' operators.

    Parameters
    ----------
    query : string that contains the words of a query

    Returns
    -------
    operator : string that contains the 'and' or 'or' operator if found, else it contains None.

    '''
    words_list = query.split()
    found = False
    for word in words_list:
        if (word == "and"):          
            operator = "and"
            found = True
        elif (word == "or"):
            operator = "or"
            found = True
    
    if (found):
        return operator
    else:
        return None 
  

from itertools import dropwhile # Used for splitting the lists of words
def intersect(query):
    '''
    Converts a query that has a complex where conditions combined with 'and' operator
    into two separate queries with simpler conditions.

    Parameters
    ----------
    query : string that contains the query

    Returns
    -------
    processed_query1 : string with the query that contains the first simpler condition.
    processed_query2 : string with the query that contains the second simpler condition.
    '''
    words_list = query.split() #split the query string to get the words
    print(words_list)
    # Using dropwhile to split into the second part of the query that contains the conditions
    query_part2 = list(dropwhile(lambda x: x != 'where', words_list))[1:]
    print("query_part2")
    print(query_part2)

    
    # Get difference between two lists (query_part1 = words_list - query_part2)
    query_part1 = [x for x in words_list if x not in query_part2]

    print("query_part1")
    print(query_part1)
    # converting to list
    query_part1 = list(query_part1)
    query_part2 = list(query_part2)
    print("query_part1_list")
    print(query_part1)
    print("query_part2List")
    print(query_part1)
    
    # Using dropwhile to split into the conditions
    condition2 = list(dropwhile(lambda x: x != 'and', query_part2))[1:]
    
    
    # Get difference between two lists (condition1 = query_part2 - condition2)
    condition1 = [x for x in query_part2 if x not in condition2]
    
    # Removing 'split' string
    condition1.remove('and')
    
    # Converting to list
    condition1 = list(condition1)
    condition2 = list(condition2)
    print("condition1list")
    print(condition1)
    print("condition2list")
    print(condition2)
    
    # Create the new queries
    query1 = query_part1 + condition1
    query2 = query_part1 + condition2
    print("query1")
    print(query1)
    print("query2")
    print(query2)
    
    # Convert the queries into strings and add ' ;' at the end.
    processed_query1 = " ".join(query1)
    if (processed_query1[-1] != ';'):
        processed_query1 += ' ;'
    
    processed_query2 = " ".join(query2)
    if (processed_query2[-1] != ';'):
        processed_query2 += ' ;'
  
    return processed_query1, processed_query2

    
def unite(query):
    '''
    Converts a query that has a complex where conditions combined with 'or' operator
    into two separate queries with simpler conditions.

    Parameters
    ----------
    query : string that contains the query

    Returns
    -------
    processed_query1 : string with the query that contains the first simpler condition.
    processed_query2 : string with the query that contains the second simpler condition.
    '''
    words_list = query.split() #split the query string to get the words
    print(words_list)
    # Using dropwhile to split into the second part of the query that contains the conditions
    query_part2 = list(dropwhile(lambda x: x != 'where', words_list))[1:]
    print("query_part2")
    print(query_part2)

    
    # Get difference between two lists (query_part1 = words_list - query_part2)
    query_part1 = [x for x in words_list if x not in query_part2]

    print("query_part1")
    print(query_part1)
    # converting to list
    query_part1 = list(query_part1)
    query_part2 = list(query_part2)
    print("query_part1_list")
    print(query_part1)
    print("query_part2List")
    print(query_part1)
    
    # Using dropwhile to split into the conditions
    condition2 = list(dropwhile(lambda x: x != 'or', query_part2))[1:]
    
    
    # Get difference between two lists (condition1 = query_part2 - condition2)
    condition1 = [x for x in query_part2 if x not in condition2]
    
    # Removing 'split' string
    condition1.remove('or')
    
    # Converting to list
    condition1 = list(condition1)
    condition2 = list(condition2)
    print("condition1list")
    print(condition1)
    print("condition2list")
    print(condition2)
    
    # Create the new queries
    query1 = query_part1 + condition1
    query2 = query_part1 + condition2
    print("query1")
    print(query1)
    print("query2")
    print(query2)
    
    # Convert the queries into strings and add ' ;' at the end.
    processed_query1 = " ".join(query1)
    if (processed_query1[-1] != ';'):
        processed_query1 += ' ;'
    
    processed_query2 = " ".join(query2)
    if (processed_query2[-1] != ';'):
        processed_query2 += ' ;'
  
    return processed_query1, processed_query2 

        
      
def interpret(query):
    '''
    Interpret the query.
    '''
    kw_per_action = {'create table': ['create table'],
                     'drop table': ['drop table'],
                     'cast': ['cast', 'from', 'to'],
                     'import': ['import', 'from'],
                     'export': ['export', 'to'],
                     'insert into': ['insert into', 'values'],
                     'select': ['select', 'from', 'where', 'distinct', 'order by', 'limit'],
                     'lock table': ['lock table', 'mode'],
                     'unlock table': ['unlock table', 'force'],
                     'delete from': ['delete from', 'where'],
                     'update table': ['update table', 'set', 'where'],
                     'create index': ['create index', 'on', 'using'],
                     'drop index': ['drop index'],
                     'create view' : ['create view', 'as']
                     }

    if query[-1]!=';':
        query+=';'
    
    query = query.replace("(", " ( ").replace(")", " ) ").replace(";", " ;").strip()

    # Find if there is either 'and' or 'or' operator.
    operator_in_query = check_operator(query)
    
    # If the operator == 'and' or 'or'
    if (operator_in_query != None):
        if (operator_in_query == 'and'):
            # Get the simpler queries for each condition
            query1, query2 = intersect(query)
            
            query_plans=[] # The list that will contain the query plan for each simpler query
            
            # For the query plan of the first query
            for kw in kw_per_action.keys():
                print(kw)
                print(query1)
                if query1.startswith(kw):
                    action = kw
                    print("inside")
            
            # For the query plan of the second query
            for kw in kw_per_action.keys():
                print("q2 " + kw)
                print(query2)
                if query2.startswith(kw):
                    action = kw
                    print("inside")
            
            # Create the query plans
            query_plans.append(create_query_plan(query1, kw_per_action[action]+[';'], action))
            query_plans.append(create_query_plan(query2, kw_per_action[action]+[';'], action))
            
            #query_plans.append(operator_in_query)
            return query_plans, operator_in_query # Return the query plans and the found operator
        
        elif (operator_in_query == 'or'):
            # Get the simpler queries for each condition
            query1, query2 = unite(query)
            
            query_plans=[] # The list that will contain the query plan for each simpler query
            
            # For the query plan of the first query
            for kw in kw_per_action.keys():
                print(kw)
                print(query1)
                if query1.startswith(kw):
                    action = kw
                    print("inside")
            
            # For the query plan of the second query
            for kw in kw_per_action.keys():
                print("q2 " + kw)
                print(query2)
                if query2.startswith(kw):
                    action = kw
                    print("inside")
            
            # Create the query plans
            query_plans.append(create_query_plan(query1, kw_per_action[action]+[';'], action))
            query_plans.append(create_query_plan(query2, kw_per_action[action]+[';'], action))
            
            #query_plans.append(operator_in_query)
            return query_plans, operator_in_query # Return the query plans and the found operator

    for kw in kw_per_action.keys():
        if query.startswith(kw):
            action = kw

    return create_query_plan(query, kw_per_action[action]+[';'], action), operator_in_query # Return the query plan and None for the operator_in_query variable

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

from tabulate import tabulate # for 'AND', 'OR' to print results
if __name__ == "__main__":
    fname = os.getenv('SQL')
    dbname = os.getenv('DB')

    db = Database(dbname, load=True)

    

    if fname is not None:
        for line in open(fname, 'r').read().splitlines():
            if line.startswith('--'): continue
            if line.startswith('explain'):
                #dic = interpret(line.removeprefix('explain '))
                temp = interpret(line.removeprefix('explain '))
                if (type(temp) is tuple):
                    dic = temp[0]
                else:
                    dic = temp
                pprint(dic, sort_dicts=False)
            else :
                dic = interpret(line.lower())
                print(dic)
                if (type(dic) is tuple): # Keep only the dictionary
                    dic = dic[0]
                
                print(dic)
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
            if line=='exit':
                break
            if line.split(' ')[0].removesuffix(';') in ['lsdb', 'lstb', 'cdb', 'rmdb']:
                interpret_meta(line)
            elif line.startswith('explain'):
                dic = interpret(line.removeprefix('explain '))
                if (type(dic) is tuple):
                    dic = dic[0] # Keep only the dictionary
                pprint(dic, sort_dicts=False)
            else:
                dic, operator_in_query = interpret(line) # Get the query plan and the operator_in_query if found
                
                # If the query contains the 'and' operator
                if (operator_in_query == 'and'):
                    results=[]
                    for query_plan in dic: # For each query plan of each simpler query
                        results.append(execute_dic(query_plan)) # Execute the dictionary and get the results
                        
                    non_none_rows=[]
                    for query_result in results: # For the results of each simpler query
                        rows, headers = query_result.results_rows_headers() # Get the header and the rows of the query results
                        non_none_rows.append(rows) 
                    
                    condition1_results = []
                    for row in non_none_rows[0]:
                        condition1_results.append(row) # Find all the rows of records that the execution of the first simpler query returned
                    
                    condition2_results = []
                    for row in non_none_rows[1]:
                        condition2_results.append(row) # Find all the rows of records that the execution of the second simpler query returned
                    
                    # Find the common rows between the simpler queries
                    intersection = set(tuple(x) for x in condition1_results).intersection(set(tuple(x) for x in condition2_results))
                    final_results = list(intersection) # Convert the set to list
                    
                    print(tabulate(final_results[:None], headers=headers)+'\n') # Print the results 
                
                elif (operator_in_query == 'or'): # If the query contains the 'or' operator
                    results=[]
                    for query_plan in dic: # For each query plan of each simpler query
                        results.append(execute_dic(query_plan)) # Execute the dictionary and get the results
                        
                    non_none_rows=[]
                    for query_result in results: # For the results of each simpler query
                        rows, headers = query_result.results_rows_headers() # Get the header and the rows of the query results
                        non_none_rows.append(rows) 
                    
                    condition1_results = []
                    for row in non_none_rows[0]:
                        condition1_results.append(row) # Find all the rows of records that the execution of the first simpler query returned
                    
                    condition2_results = []
                    for row in non_none_rows[1]:
                        condition2_results.append(row) # Find all the rows of records that the execution of the second simpler query returned
                    
                    # Return all the items from both sets
                    union = set(tuple(x) for x in condition1_results).union(set(tuple(x) for x in condition2_results))
                    final_results = list(union) # Convert the set to list
                    
                    print(tabulate(final_results[:None], headers=headers)+'\n') # Print the results 
                else:
                    result = execute_dic(dic)
                    if isinstance(result,Table):
                        result.show()
                
                
        except Exception:
            print(traceback.format_exc())
