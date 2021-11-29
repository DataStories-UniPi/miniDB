from database import Database
import os
import re
from pprint import pprint
import sys
import readline
import traceback
import shutil

# art font is big
art = '''
             _         _  _____   ____  
            (_)       (_)|  __ \ |  _ \     
  _ __ ___   _  _ __   _ | |  | || |_) |
 | '_ ` _ \ | || '_ \ | || |  | ||  _ < 
 | | | | | || || | | || || |__| || |_) |
 |_| |_| |_||_||_| |_||_||_____/ |____/   2021 - v3.1                               
'''   

def search_between(s, first, last):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
    except:
        return
    return s[start:end].strip()


def create_query_plan(query, keywords, action):

    dic = {val: None for val in keywords}

    ql = query.split(' ')

    kw_in_query = []
    for i in range(len(ql)-1):
        if ql[i] in keywords:
            kw_in_query.append(ql[i])
        elif f'{ql[i]} {ql[i+1]}' in keywords:
            kw_in_query.append(f'{ql[i]} {ql[i+1]}')
    kw_in_query.append(';')
    # print(kw_in_query)

    for kw1, kw2 in zip(kw_in_query,kw_in_query[1:]):
        dic[kw1] = search_between(query,kw1,kw2)

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
        # dic['import table'] = dic.pop(action)

    if action=='insert into':
        if dic['values'][0] == '(' and dic['values'][-1] == ')':
            dic['values'] = dic['values'][1:-1]
        else:
            raise ValueError('Your parens are not right m8')
        
    return dic

def evaluate_from_clause(dic):
    join_types = ['inner','left', 'right', 'full']
    if dic['from'][0] == '(' and dic['from'][-1] == ')':
        subquery = dic['from'][1:-1]
        dic['from'] = interpret(subquery)

    if 'join' in dic['from']:
        join_dic = {}
        join_sent = dic['from'].split(' ')
        if join_sent.count('join')>1:
            raise ValueError('Too many joins')
        jidx = join_sent.index('join')
        if join_sent[jidx-1] in join_types:
            join_dic['join'] = join_sent[jidx-1]
            join_dic['left'] = join_sent[jidx-2]
        else:
            join_dic['join'] = 'inner'
            join_dic['left'] = join_sent[jidx-1]
        join_dic['right'] = join_sent[jidx+1]
        join_dic['on'] = ''.join(join_sent[join_sent.index('on')+1:])
        dic['from'] = join_dic
        # pass
        
    return dic

def interpret(query):
    kw_per_action = {'create table': ['create table'],
                     'drop table': ['drop table'],
                     'cast': ['cast', 'from', 'to'],
                     'import': ['import', 'from'],
                     'export': ['export', 'to'],
                     'insert into': ['insert into', 'values'],
                     'select': ['select', 'from', 'where', 'order by', 'top'],
                     'lock table': ['lock table', 'mode'],
                     'unlock table': ['unlock table'],
                     'delete from': ['delete from', 'where'],
                     'update table': ['update table', 'set', 'where'],
                    #  'create database': ['create database'],
                    #  'drop database': ['drop database'],
                    #  'save database': ['save database'],
                    #  'load database': ['load database'],
                     'create index': ['create index', 'on', 'using'],
                     'drop index': ['drop index']
                     }

    if query[-1]!=';':
        query+=';'

    for kw in kw_per_action.keys():
        if query.startswith(kw):
            action = kw

    return create_query_plan(query, kw_per_action[action], action)

def execute_dic(dic):
    
    # db.create_table('classroom', ['building', 'room_number', 'capacity'], [str,str,int])
    # # insert 5 rows
    # db.insert('classroom', ['Packard', '101', '500'])
    # db.insert('classroom', ['Painter', '514', '10'])
    # db.insert('classroom', ['Taylor', '3128', '70'])
    # db.insert('classroom', ['Watson', '100', '30'])
    # db.insert('classroom', ['Watson', '120', '50'])
    for key in dic.keys():
        if isinstance(dic[key],dict):
            dic[key] = execute_dic(dic[key])
    
    action = list(dic.keys())[0].replace(' ','_')
    # try:
    return getattr(db, action)(*dic.values())
    # except AttributeError:
    #     raise NotImplementedError("Class `{}` does not implement `{}`".format(db.__class__.__name__, action))

def interpret_meta(command):
    """
    lsdb - list databases
    lstb - list tables
    """
    # global db
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
    # create db with name "smdb"
    fname = os.getenv('SQL')
    dbname = os.getenv('DB')
    sbs = bool(int(os.getenv('SBS',0)))

    db = Database(dbname, load=True)

    if fname is not None:
        for line in open(fname, 'r').read().splitlines():
            if line.startswith('--'): continue
            dic = interpret(line.lower())
            pprint(dic, sort_dicts=False)
            result = execute_dic(dic)

            if sbs: 
                if input()!='x':
                    result = execute_dic(dic)
            if result is not None:
                result.show()
    else:
        print(art)
        while 1:
            try:
                line = input(f'({db._name})> ').lower()
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
                    if result is not None:
                        result.show()
            except Exception:
                print(traceback.format_exc())
                # print(e)


                
                
