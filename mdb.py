import os
import re
from pprint import pprint
import sys
import readline
import traceback
import shutil

sys.path.append('miniDB')

from miniDB.database import Database
from miniDB.table import Table
from miniDB.query_plans import multiple_query_plans
from miniDB.evaluate_query_plans import evaluate_query_plans
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
        dic['as'] = interpret(dic['as'])

    if action=='select':
        dic = evaluate_from_clause(dic)

        if dic['where'] is not None:
            dic = evaluate_where_clause(dic)
        
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
        temp_args = args.replace('primary key', '')[1:-1] # remove primary key and parentheses
        temp_args = temp_args.replace('unique', '') # remove unique
        arglist = [val.strip().split(' ') for val in temp_args.split(',')]
        dic['column_names'] = ','.join([val[0] for val in arglist])
        dic['column_types'] = ','.join([val[1] for val in arglist])
        
        if 'primary key' in args:
            arglist = args[1:-1].split(' ')
            dic['primary key'] = arglist[arglist.index('primary')-2]
        else:
            dic['primary key'] = None
            
        if 'unique' in args:
            arglist = args[1:-1].split(',')
            arglist = [val.strip().split(' ') for val in arglist]
            column_names = [val[0] for val in arglist if len(val)>2 and val[2]=='unique']
            dic['unique_columns'] = ','.join(column_names)
        else:
            dic['unique_columns'] = None

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

    if action=='delete from':
        if dic['where'] is not None:
            dic = evaluate_where_clause(dic)
        else:
            dic['where'] = None

    if action=='update':
        if dic['where'] is not None:
            dic = evaluate_where_clause(dic)
        else:
            dic['where'] = None

    if action=='create index':
        # Check if 'on' clause is not None and if is of the form 'table_name (column_name)'
        if dic['on'] is not None and '(' in dic['on'] and ')' in dic['on'] and dic['on'].count('(') == dic['on'].count(')') == 1:
            on_clause = dic['on'].split('(')
            table_name = on_clause[0].strip()
            column_name = on_clause[1][:-1].strip()
            dic['on'] = { 'table_name': table_name, 'column_name': column_name }
        else:
            raise ValueError('\nWrong syntax: "on" clause must be of the form "table_name (column_name, ...)"\n')
        
    return dic

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
        and_idx = [i+on_idx+1 for i,word in enumerate(from_split[on_idx+1:]) if word=='and']
        if and_idx:
            join_dic['on'] = {'and':{'left':' '.join(from_split[on_idx+1:and_idx[0]]),'right':' '.join(from_split[and_idx[0]+1:])}}
        else:
            join_dic['on'] = ''.join(from_split[on_idx+1:])

        if join_dic['left'].startswith('(') and join_dic['left'].endswith(')'):
            join_dic['left'] = interpret(join_dic['left'][1:-1].strip())

        if join_dic['right'].startswith('(') and join_dic['right'].endswith(')'):
            join_dic['right'] = interpret(join_dic['right'][1:-1].strip())

        dic['from'] = join_dic
        
    return dic

def evaluate_where_clause(dic):
    '''
    Evaluate the part of the query (argument or subquery) that is supplied as the 'where' argument
    '''
    def convert_list_to_dict(lst):
        '''
        Converting the list of dictionaries to the desired dictionary,
        where each key ('OPR') will later be replaced with an operator ('and' or 'or').
        '''
        if len(lst) == 1:
            return {'OPR': lst[0]}
        else:
            return {'OPR': {'left': lst[0]['left'], 'right': convert_list_to_dict(lst[1:])}}
    
    def build_list(oprt_idx, where_split):
        '''
        Building a list of dictionaries, where each dictionary contains
        the left and right sides of the operator ('and' or 'or').
        '''
        if oprt_idx:
            oprt_dic = {}
            
            if ' '.join(where_split[:oprt_idx[0]]).startswith('not '):
                oprt_dic['left'] = ' '.join(where_split[:oprt_idx[0]])
            else:
                oprt_dic['left'] = ' '.join(where_split[oprt_idx[0]-1:oprt_idx[0]])
                if oprt_dic['left'] == ')':
                    pos  = where_split[:oprt_idx[0]].index('(')
                    oprt_dic['left'] = ' '.join(where_split[pos:oprt_idx[0]])
                if ' '.join(where_split[:oprt_idx[0]]).__contains__('between'):
                    btwn_idx = where_split[:oprt_idx[0]].index('between')
                    if not in_paren(where_split, btwn_idx):
                        raise ValueError(f'\nWrong syntax: "between" clause must be in parentheses.\n')

            oprt_dic['left'] = evaluate_where_clause( { 'where':  oprt_dic['left'] } )['where']
            oprt_dic['right'] = ' '.join(where_split[oprt_idx[0]+1:])
            oprt_dic['right'] = evaluate_where_clause( { 'where':  oprt_dic['right'] } )['where']
            List.append(oprt_dic)
    
    def put_paren_in_oprt_and(where_split):
        '''
        Placing parentheses around the 'and' operator, if it is not already within parentheses, when an 'or' operator exists,
        helps ensure the proper priority of the operators ('and' and 'or'). This also works for some 'between' clauses, 
        but it is recommended to always put parentheses around the 'between' clause.
        '''
        def find_idx():
            '''
            Finding the indices of the 'and' and 'or' operators.
            '''
            and_idx = [i for i,word in enumerate(where_split) if word=='and' and not in_paren(where_split,i)]
            or_idx = [i for i,word in enumerate(where_split) if word=='or' and not in_paren(where_split,i)]
            oprt_idx = and_idx + or_idx
            oprt_idx.sort()
            return oprt_idx, and_idx, or_idx
        
        oprt_idx, and_idx, or_idx = find_idx()
        previous_and = False
        idx_for_paren = 0
        while len(oprt_idx) > 0 and and_idx and or_idx:
            idx = oprt_idx.pop(0)
            if where_split[idx] == 'and' and not previous_and:
                previous_and = True
                where_split.insert(idx_for_paren, '(')
                if len(oprt_idx) == 0:
                    where_split.append(')')
                    break
                elif where_split[idx+2] == 'not':
                    where_split.insert(idx+4, ')')
                else:
                    where_split.insert(idx+3, ')')
                oprt_idx, and_idx, or_idx = find_idx()
            elif where_split[idx] == 'and' and previous_and:
                where_split.pop(idx-1)
                where_split.insert(idx+1, ')')
            else:
                previous_and = False
                idx_for_paren = idx+1
        return where_split
    
    where_split = put_paren_in_oprt_and(dic['where'].split(' '))
    
    '''
    not/between/and/or operators not in parentheses.
    '''
    not_idx = [i for i, word in enumerate(where_split) if word == 'not' and not in_paren(where_split, i)]
    btwn_idx = [i for i, word in enumerate(where_split) if word == 'between' and not in_paren(where_split, i)]
    oprt_idx = [i for i,word in enumerate(where_split) if (word=='or' or word=='and') and not in_paren(where_split,i)] # oprt_idx contains the indices of operators 'and' and 'or'.
    
    '''
    Checking if the 'where' clause is within parentheses or if it starts with
    'not' operator outside of parentheses and is followed by parentheses.
    '''
    if not oprt_idx and (where_split[0] == '(' or where_split[0] == 'not' and where_split[1] == '(') and where_split[-1] == ')':
        if not_idx:
            dic['where'] = { 'not': evaluate_where_clause( { 'where': ' '.join(where_split).removeprefix('not ') } )['where'] }
            return dic
        else:
            dic['where'] = evaluate_where_clause( { 'where': ' '.join(where_split[1:-1]) } )['where']
            return dic
      
    if btwn_idx and len(oprt_idx) == 1:
        '''
        If the 'between' operator exists and there is only one operator ('and'),
        evaluate the 'between' clause and return the result.
        '''
        if len(where_split) < 5:
            raise Exception('\nWrong syntax of "between" clause.\n')
        dic['where'] = { 'column': where_split[btwn_idx[0]-1], 'between': evaluate_where_clause( { 'where':  ' '.join(where_split[btwn_idx[0]+1:]) } )['where'] }
        return dic

    if oprt_idx:
        '''
        If there are operators ('and' or 'or'), we build a list of dictionaries and then convert it into a dictionary.
        The 'OPR' string is used as a placeholder for the actual operator and then is replaced with the correct operator from left to right.
        '''
        List = []
        build_list(oprt_idx, where_split)
        oprt_dic = str(convert_list_to_dict(List))
        oprt_words = [where_split[i] for i in oprt_idx]

        '''
        Replace the 'OPR' string with the actual operator ('and' or 'or').
        '''
        oprt_dic = oprt_dic.replace('OPR', oprt_words[0], 1)
        dic['where'] = dict(eval(oprt_dic))
        return dic
    
    if not_idx:
        '''
        If the simple 'not' operator exists, create a dictionary with the 'not' operator
        where the key is 'not' and the value is the right side of the 'not' operator.
        '''
        dic['where'] = {'not': where_split[not_idx[0]+1]}
        return dic
    
    dic['where'] = ''.join(where_split)
    return dic

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
                     'update': ['update', 'set', 'where'],
                     'create index': ['create index', 'on', 'using'],
                     'drop index': ['drop index'],
                     'create view' : ['create view', 'as']
                     }


    if query[-1]!=';':
        query+=';'
    
    query = query.replace("(", " ( ").replace(")", " ) ").replace(";", " ;").strip()

    for kw in kw_per_action.keys():
        if query.startswith(kw):
            action = kw

    return create_query_plan(query, kw_per_action[action]+[';'], action)

def execute_dic(dic):
    '''
    Execute the given dictionary
    '''
    for key in dic.keys():
        if isinstance(dic[key], dict) and key == 'from':
            dic[key] = execute_dic(dic[key])
    
    action = list(dic.keys())[0].replace(' ','_')
    return getattr(db, action)(*dic.values())

def interpret_meta(command):
    '''
    Interpret meta commands. These commands are used to handle DB stuff, something that can not be easily handled with mSQL given the current architecture.

    The available meta commands are:

    lsdb - list databases
    lstb - list tables
    cdb - change/create database
    rmdb - delete database
    '''
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
            if line=='exit;':
                print('\nbye!')
                break
            if line.split(' ')[0].removesuffix(';') in ['lsdb', 'lstb', 'cdb', 'rmdb']:
                interpret_meta(line)
            elif line.startswith('explain'):
                dic = interpret(line.removeprefix('explain '))
                queries = multiple_query_plans(dic)
                evaluate_query_plans(db,queries)
                #pprint(dic, sort_dicts=False)
            else:
                dic = interpret(line)
                #if 'select' in dic.keys() and not dic['from'].startswith('meta'):
                queries = multiple_query_plans(dic)
                dic = evaluate_query_plans(db,queries)
                result = execute_dic(dic)
                if isinstance(result,Table):
                    result.show()
        except Exception as e:
            print(traceback.format_exc())
            print(e)