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
    for i in range(len(ql)):
        if ql[i] in keywords and not in_paren(ql, i):
            kw_in_query.append(ql[i])
            kw_positions.append(i)
        elif i!=len(ql)-1 and f'{ql[i]} {ql[i+1]}' in keywords and not in_paren(ql, i):
            kw_in_query.append(f'{ql[i]} {ql[i+1]}')
            kw_positions.append(i+1)
   # print(kw_in_query)
   # print(kw_positions)


    for i in range(len(kw_in_query)-1):
        dic[kw_in_query[i]] = ' '.join(ql[kw_positions[i]+1:kw_positions[i+1]])

    if action=='select':
        dic = evaluate_from_clause(dic)
        
        if dic['order by'] is not None:
            dic['from'] = dic['from'].removesuffix(' order')
            if 'desc' in dic['order by']:
                dic['desc'] = True
            else:
                dic['desc'] = False
            dic['order by'] = dic['order by'].removesuffix(' asc').removesuffix(' desc')
            
        else:
            dic['desc'] = None

    if action=='create table':
        args = dic['create table'][dic['create table'].index('('):dic['create table'].rindex(')')+1]
        dic['create table'] = dic['create table'].removesuffix(args).strip()
        arg_nopk = args.replace('primary key', '')[1:-1]
        arglist = [val.strip().split(' ') for val in arg_nopk.split(',')]
        dic['column_names'] = ','.join([val[0] for val in arglist])
        dic['column_types'] = ','.join([val[1] for val in arglist])
#insert here
        if 'foreign key' in args:
            print(args)
            fkeyres = [i.start() for i in re.finditer('foreign key',args )]
            frefres = [i.start() for i in re.finditer('references',args )]                        
            if len(fkeyres)==len(frefres):
                full = args[fkeyres[0]:]                    
                l=[val.strip().split(' ') for val in full.split(',')]
                dic['foreign keys'] = [val[2] for val in l]
                dic['foreign table'] = [val[4] for val in l]
                dic['foreign table key'] = [val[6] for val in l]
                print(dic['foreign keys'])
                print(dic['foreign table'])
                print(dic['foreign table key'])
                for i in range(len(dic['foreign keys'])):
                    myline = 'select '+dic['foreign table key'][i]+  ' from '+ dic['foreign table'][i]
                    print(myline)
                    mydic = interpret(myline)
                    myresult = execute_dic(mydic)
                    if isinstance(myresult,Table):
                        print('all good')
                    else:
                        raise ValueError('The foreign key does not exist in the corresponding table')
                #del dic['foreign keys']
                #del dic['foreign table']
                #del dic['foreign table key']
                '''
                k=[]
                for i in range(len(fkeyres)):
                    full = args[fkeyres[i]:]                    
                  #  print(full)
                    l=[val.strip().split(' ') for val in full.split(',')]
                    print(l)
                    #dic['foreign keys '] = ','.join([val[2] for val in l])
                    dic['foreign keys'] = [[val[2] for val in l]]
                    dic['foreign tables'] = [[val[4] for val in l]]
                    print(dic['foreign keys'])
                    print(dic['foreign tables'])
                    print('To i einai : '+ str(i))
                    #print('To len einai : '+ str(len(l[i])))
                    print (l[0][2])
                    for j in range(2,8,2):
                        print('To j einai : '+ str(j))
                        #k.append(l[i][j],l[i][5],l[i][7])                    
                        k.append(l[0][j])                    
                print(k)
             #   print(fkeyres)
             #   print(frefres)
             #   print('ok')
             '''
            else:
                raise ValueError('Your parens are not right m8')
            '''
            myforeignkeyindex = args.find('foreign key')        
            myreferenceskeyindex=args.find('references')        
            print(args)
            myargs=args[myforeignkeyindex:myreferenceskeyindex]
            print (myargs)
            myreferences= args[myreferenceskeyindex:]
            print (myreferences)
            '''



      
        if 'primary key' in args:
            arglist = args[1:-1].split(' ')
            dic['primary key'] = arglist[arglist.index('primary')-2]
        else:
            dic['primary key'] = None
    
    if action=='import': 
        dic = {'import table' if key=='import' else key: val for key, val in dic.items()}

    if action=='insert into':
        if dic['values'] is not None:
            if dic['values'][0] == '(' and dic['values'][-1] == ')':
                dic['values'] = dic['values'][1:-1]                            
        elif dic['select'] is not None:
            dic['select']='select '+dic['select']
            k=interpret(dic['select'])            
            result=execute_dic(k)
            non_none_rows = [row for row in result.data if any(row)] 
            print(len(non_none_rows))
            for i_row in non_none_rows:               
                my_lst_str = ','.join(map(str, i_row))            
                query='insert into ' + dic['insert into'] + ' values ('+ my_lst_str +')'               
                k=interpret(query)            
                if 'select' in k: del k['select']                          
                result=execute_dic(k)
            if 'select' in dic: del dic['select']                                              
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
    kw_per_action = {'create table': ['create table'],
                     'drop table': ['drop table'],
                     'cast': ['cast', 'from', 'to'],
                     'import': ['import', 'from'],
                     'export': ['export', 'to'],
                     #'insert into': ['insert into', 'values'],
                     'insert into': ['insert into', 'values','select'],
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
        if isinstance(dic[key],dict):
            dic[key] = execute_dic(dic[key])
    action = list(dic.keys())[0].replace(' ','_')   
 #   print(*dic.values()) 
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