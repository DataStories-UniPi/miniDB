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
 |_| |_| |_||_||_| |_||_||_____/ |____/   2021 - v3.2                               
'''   
"""
          GENIKA TI EXW KATALAVEI MEXRI TWRA
          
TO PROGRAMMA PAIRNEI SAN INPUT TO QUERY TOY XRHSTH.
STH SYNEXEIA ELEGXEI TO PRWTO KEYWORD. TO KATHE KEYWORD THS SQL
EXEI ALLA PROVLEPOMENA KEYWORDS POU MPOREI NA AKOLOYTHHSOYN.
AYTA TA PROVLEPOMENA KEYWORDS ELEGXETAI AN YPARXOYN MESA SE OLO TO QUERY
POU EDWSE O XRHSTHS. OSA YPARXOYN ONTWS APOTHIKEVONTAI SE LISTA/LEKSIKO.
SE ENA LEKSIKO TELIKA EXOUME TA KEYWORDS MAZI ME AYTA POY EKTELOUN
PX {'select': '*', 'from': 'table', ...}

"""

def search_between(s, first, last):
    '''
    Search in 's' for the substring that is between 'first' and 'last'
    '''
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
    except:
        return
    return s[start:end].strip() # to strip kovei ta kena prin kai meta to string

def in_paren(qsplit, ind):
    '''
    Split string on space and return whether the item in index 'ind' is inside a parentheses
    '''
    return qsplit[:ind].count('(')>qsplit[:ind].count(')') #an oi parenthesieis '(' einai perissoteres apo oti oi ')' aristera enos keyword sth lista
                                                           #ayto shmainei oti to keyword sto query vrisketai se parenthesh

def create_query_plan(query, keywords, action): 
    '''
    Given a query, the set of keywords that we expect to pe present and the overall action, return the query plan for this query.

    This can and will be used recursively
    '''

    dic = {val: None for val in keywords if val!=';'} #vazoyme sto leksiko san keys ola ta keywords(ektos apo ;) me value none sto kathena

    ql = [val for val in query.split(' ') if val !=''] #vazoyme sth lista oles tis lekseis toy query(xwrizontai me keno)

    kw_in_query = [] #h lista tha periexei ta koina keywords ths listas "keywords" kai toy query
    kw_positions = [] #ta index twn keywords sto query
    for i in range(len(ql)):
        if ql[i] in keywords and not in_paren(ql, i): #an h i-osth leksh toy query einai keyword kai den einai se parenthesh valth sth lista mazi me to index ths
            kw_in_query.append(ql[i])
            kw_positions.append(i)
        elif i!=len(ql)-1 and f'{ql[i]} {ql[i+1]}' in keywords and not in_paren(ql, i): #an i-osth kai (i+1)-osth leksh tou query apotelei keyword kai den einai se parenthesh
            kw_in_query.append(f'{ql[i]} {ql[i+1]}')
            kw_positions.append(i+1) #apothikevoyme to index ths deyterhs lekshs toy keyword


    for i in range(len(kw_in_query)-1):
        dic[kw_in_query[i]] = ' '.join(ql[kw_positions[i]+1:kw_positions[i+1]]) #ME EKAPSES THEODOROPOYLE, ANTE NA TO EKSHGHSW AYTO
                                                                                
          '''
                    TRUST ME AT YOUR OWN RISK
          1.vriskoume to i-osto keyword toy query
          2.to keyword ayto apotelei key toy leksikou dic
          3.thetoume to antistoixo value toy dic iso me thn/tis timh/times poy antiproswpeyei to keyword
          px
          SELECT * FROM TABLE WHERE ID=3
          SELECT c1, c2 FROM TABLE WHERE ID=3
          
          Gia to select tha exoyme: {'select': '*'} kai {'select': 'c1, c2'}
          '''
    if action=='select':
        dic = evaluate_from_clause(dic)
        
        if dic['order by'] is not None:
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
    join_types = ['inner', 'left', 'right', 'full']
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
    '''
    Interpret the query.
    '''
    kw_per_action = {'create table': ['create table'],    #dict poy periexei ta actions san keys kai ta keywords twn actions san values
                     'drop table': ['drop table'],
                     'cast': ['cast', 'from', 'to'],
                     'import': ['import', 'from'],
                     'export': ['export', 'to'],
                     'insert into': ['insert into', 'values'],
                     'select': ['select', 'from', 'where', 'order by', 'top'],
                     'lock table': ['lock table', 'mode'],
                     'unlock table': ['unlock table', 'force'],
                     'delete from': ['delete from', 'where'],
                     'update table': ['update table', 'set', 'where'],
                     'create index': ['create index', 'on', 'using'],
                     'drop index': ['drop index']
                     }

    if query[-1]!=';':
        query+=';'
    
    query = query.replace("(", " ( ").replace(")", " ) ").replace(";", " ;").strip() #oi parentheseis kai to ; einai pithano na einai kollhta sta keywords.
                                                                                     #opote prosthetoyme kena kai vgazoyme ta kena sthn arxh kai telos toy query
    for kw in kw_per_action.keys(): #thetoume to action iso me to prwto keyword toy query an ayto yparxei san key sto dict
        if query.startswith(kw):
            action = kw

    return create_query_plan(query, kw_per_action[action]+[';'], action) #to deytero orisma apotelei to value tou action sto dict(lista me keywords + to ;)

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
    fname = os.getenv('SQL')
    dbname = os.getenv('DB')

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
                if line.startswith('.'):
                    interpret_meta(line)
                elif line.startswith('explain'):
                    dic = interpret(line.removeprefix('explain '))
                    pprint(dic, sort_dicts=False)
                else:
                    dic = interpret(line)
                    result = execute_dic(dic)
                    if isinstance(result,Table):
                        result.show()
            except Exception:
                print(traceback.format_exc())
