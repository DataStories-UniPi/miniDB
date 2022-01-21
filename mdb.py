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
#Art font is "big"
art = '''
             _         _  _____   ____  
            (_)       (_)|  __ \ |  _ \     
  _ __ ___   _  _ __   _ | |  | || |_) |
 | '_ ` _ \ | || '_ \ | || |  | ||  _ < 
 | | | | | || || | | || || |__| || |_) |
 |_| |_| |_||_||_| |_||_||_____/ |____/   2021 - v3.2                               
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
    
    query -> our command 
    keywords -> kw_per_action table from before
    action -> our first word
    '''
    #It makes dic from keywords (aka kw_per_action table): {'select': None, 'from': None, 'where': None, 'order by': None, 'top': None}
    dic = {val: None for val in keywords if val!=';'}

    #A tables with each word from query (our command): e.g. ql[select, from, *, classroom]
    ql = [val for val in query.split(' ') if val !='']

    #2 empty tables are created here
    kw_in_query = []
    kw_positions = []
    
    #For repeats itself as many times as the lenght of ql table
    for i in range(len(ql)):
        
        #Checks if the words from our command is in keywords (aka key_per_action)
        if ql[i] in keywords and not in_paren(ql, i):
             #If yes then it adds the word in the tables kw_in_query
            kw_in_query.append(ql[i])
            
            #It also adds the i in which the word found in the table kw_positions
            kw_positions.append(i)
        elif i!=len(ql)-1 and f'{ql[i]} {ql[i+1]}' in keywords and not in_paren(ql, i):
            kw_in_query.append(f'{ql[i]} {ql[i+1]}')
            kw_positions.append(i+1)

    #This prints these 2 tables - enable if you want to check some shit
    #print(kw_in_query)
    #print(kw_positions)

    #A new for which runs as many times as the lengh of the table kw_in_query (contains useful words of action) minus 1
    for i in range(len(kw_in_query)-1):
        #dic should look like this: 
        #{'select': '*', 'from': 'department order', 'where': None, 'order by': 'budget', 'top': None}
        dic[kw_in_query[i]] = ' '.join(ql[kw_positions[i]+1:kw_positions[i+1]])
        #print(dic)
        
    #Get ready for sql things to happen now
    #We check if the first word of our command (aka action variable) is select    
    if action=='select':
        #If yes we go into evaluate_from_clause function with dic parameter
        #There we play with the join situation
        dic = evaluate_from_clause(dic)
        
        #Checks if dic contains order by is not emptyn and has a table e.g. 'order by: capacity'
        if dic['order by'] is not None:
            #.removesuffix removes ' order' if exists
            dic['from'] = dic['from'].removesuffix(' order')
            #disc['form'] contains now the table alone e.g. select * from department order by budget desc-> classroom

            #Check if order by has a desc e.g. 'order by: capacity desc'
            if 'desc' in dic['order by']:
                dic['desc'] = True
            else:
                dic['desc'] = False
                
            #removes asc or desc from the end
            dic['order by'] = dic['order by'].removesuffix(' asc').removesuffix(' desc')
            
        else:
            dic['desc'] = None
            
        if dic['group by'] is not None:
            #.removesuffix removes ' order' if exists
            dic['from'] = dic['from'].removesuffix(' group')
            dic['desc'] = False
            dic['group by'] = dic['group by'].removesuffix(' desc')
            
        else:
            dic['desc'] = None

    if action=='create table':
        args = dic['create table'][dic['create table'].index('('):dic['create table'].index(')')+1]
        dic['create table'] = dic['create table'].removesuffix(args).strip()
        arg_nopk = args.replace('primary key', '')[1:-1]
        arglist = [val.strip().split(' ') for val in arg_nopk.split(',')]
        dic['column_names'] = ','.join([val[0] for val in arglist])
        dic['column_types'] = ','.join([val[1] for val in arglist])
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
    #Creates a table with these values
    join_types = ['inner', 'left', 'right', 'full']
    
    #from_split contains the database table: e.g. select * from classroom order by capacity
    #from_split = ['department', 'order']
    from_split = dic['from'].split(' ')
    
    #Checks for parenthesis
    if from_split[0] == '(' and from_split[-1] == ')':
        subquery = ' '.join(from_split[1:-1])
        dic['from'] = interpret(subquery)

    #Creates join_idx table
    #Checks if from_split table contains the word join
    join_idx = [i for i,word in enumerate(from_split) if word=='join' and not in_paren(from_split,i)]
    
    #Creates on_idx table
    #Checks if from_split table contains the word on
    on_idx = [i for i,word in enumerate(from_split) if word=='on' and not in_paren(from_split,i)]
    if join_idx:
        #Assigns values to join_idx and on_idx
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
    '''
    Interpret the query.
    '''
    #It adds these into the table kw_per_action
    kw_per_action = {'create table': ['create table'],
                     'drop table': ['drop table'],
                     'cast': ['cast', 'from', 'to'],
                     'import': ['import', 'from'],
                     'export': ['export', 'to'],
                     'insert into': ['insert into', 'values'],
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
                     'select': ['select', 'from', 'where', 'order by', 'group by','having', 'top'],
=======
                     'select': ['select', 'from', 'where', 'order by', 'group by', 'having', 'top'],
>>>>>>> a845cbd2fa6e26d77fe241f040c613beb60304dd
=======
                     'select': ['select', 'from', 'where', 'order by', 'group by', 'having', 'top'],
>>>>>>> a845cbd2fa6e26d77fe241f040c613beb60304dd
=======
                     'select': ['select', 'from', 'where', 'order by', 'group by', 'having', 'top'],
>>>>>>> a845cbd2fa6e26d77fe241f040c613beb60304dd
=======
                     'select': ['select', 'from', 'where', 'order by', 'group by', 'having', 'top'],
>>>>>>> a845cbd2fa6e26d77fe241f040c613beb60304dd
                     'lock table': ['lock table', 'mode'],
                     'unlock table': ['unlock table', 'force'],
                     'delete from': ['delete from', 'where'],
                     'update table': ['update table', 'set', 'where'],
                     'create index': ['create index', 'on', 'using'],
                     'drop index': ['drop index']
                     }

    #It adds ; if it doesnt exist (again) idk why
    if query[-1]!=';':
        query+=';'
    
    #It adds spaces between the words of our command -useful for later-
    query = query.replace("(", " ( ").replace(")", " ) ").replace(";", " ;").strip()

    #It checks if the first word of our command (e.g. select) fits with a word from the table kw_per_action
    for kw in kw_per_action.keys():
        if query.startswith(kw):
            action = kw

    #We return the variables: query (our command), kw_per_action (the tale), action (the first word of our command) 
    #to the create_query_plan function and we continue there
    return create_query_plan(query, kw_per_action[action]+[';'], action)

def execute_dic(dic):
    '''
    Execute the given dictionary
    '''
    #This for runs as many times as the keys of dic
    #e.g. dict_keys(['select', 'from', 'where', 'order by', 'top', 'desc'])
    for key in dic.keys():
        
        #Checks if dic[keys] are dict type (like float, int etc..)
        if isinstance(dic[key],dict):
            dic[key] = execute_dic(dic[key])
    
    action = list(dic.keys())[0].replace(' ','_')
    
    #*dic.values() is just the values of dic table
    #e.g. department None budget None True
    #print(getattr(db, action), *dic.values(), dic)
    
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
    action = command[1:].split(' ')[0].removesuffix(';')

    db_name = db._name if search_between(command, action,';')=='' else search_between(command, action,';')

    def list_databases(db_name):
        [print(fold.removesuffix('_db')) for fold in os.listdir('dbdata')]
    
    def list_tables(db_name):
        [print(pklf.removesuffix('.pkl')) for pklf in os.listdir(f'dbdata/{db_name}_db') if pklf.endswith('.pkl')\
            and not pklf.startswith('meta')]

    def change_db(db_name):
        global db
        db = Database(db_name, load=True)
    
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
    #Something with the name of DB
    fname = os.getenv('SQL')
    dbname = os.getenv('DB')

    #Loads Database
    db = Database(dbname, load=True)

    if fname is not None:
        for line in open(fname, 'r').read().splitlines():
            if line.startswith('--'): continue
            dic = interpret(line.lower())
            result = execute_dic(dic)
            if isinstance(result,Table):
                result.show()
    else:
        from prompt_toolkit import PromptSession
        from prompt_toolkit.history import FileHistory
        from prompt_toolkit.auto_suggest import AutoSuggestFromHistory

        #Prints miniDB logo
        print(art)
        
        #Loads History
        session = PromptSession(history=FileHistory('.inp_history'))
        while 1:
            try:
                #Input
                line = session.prompt(f'({db._name})> ', auto_suggest=AutoSuggestFromHistory()).lower()
                
                #Check for ; if it doesnt exit, it adds ones
                if line[-1]!=';':
                    line+=';'
            except (KeyboardInterrupt, EOFError):
                print('\nbye!')
                break
            try:
                #Some occasions like exit and .
                if line=='exit':
                    break
                if line.startswith('.'):
                    interpret_meta(line)
                elif line.startswith('explain'):
                    dic = interpret(line.removeprefix('explain '))
                    pprint(dic, sort_dicts=False)
                #This happends if our command is valid
                else:
                    #interpret runs and r
                    #Returns line into disc
                    dic = interpret(line)
                    
                    #dic looks like this now: 
                    # {'select': '*', 'from': 'department', 'where': None, 'order by': 'budget', 'top': None, 'desc': True}
                    #function execut_dic(dic) runs and we continue there
                    result = execute_dic(dic)
                    if isinstance(result,Table):
                        result.show()
            except Exception:
                print(traceback.format_exc())
