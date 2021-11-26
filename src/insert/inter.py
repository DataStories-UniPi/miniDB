import os
import re
from pprint import pprint
import sys

def match_parens(text):
    # print(text)
    d = []
    istart = []
    for i, c in enumerate(text):
        if c == '(':
            istart.append(i)
        if c == ')':
            try:
                d.append((istart.pop(),i)) 
            except IndexError:
                raise ValueError('Too many closing parentheses')
    if istart:  # check if stack is empty afterwards
        raise ValueError('Too many opening parentheses')
    
    return d[-1] if d else None




def search_between(s, first, last):

    # return re.search(fr'{a}(.*){b}', line).group(1)
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
    except:
        try:
            end = s.index( ';', start )
        except:
            return
    return s[start:end]

def next_word(line, a):
    try:
        return re.search(fr'{a}(.*)', line).group(1).split(';')[0].split(' ')[0]
    except:
        return 

def create_table(query):
    table_name = search_between(query, 'table', '(')
    column_names = [val.strip().split(' ')[0] for val in search_between(query,'(', ')').split(',')]
    column_types = [val.strip().split(' ')[1] for val in search_between(query,'(', ')').split(',')]
    print(f'creating table {table_name} with cols {column_names} and types {column_types}')

def drop_table(query):
    table_name = search_between(query,'table', ';')
    print(f'droping table -> {table_name}')

def cast_table(query):
    colname = search_between(query,'cast', 'from')
    tablename = search_between(query,'from', 'to')
    coltype = search_between(query,'to', ';')
    print(f'casting col {colname} from table {tablename} to {coltype}')

def import_table(query):
    table_name = search_between(query,'import', 'from')
    csv_name = search_between(query,'from', ';')
    print(f'importing table -> {table_name} from {csv_name}')

def export_table(query):
    table_name = search_between(query,'export', 'to')
    csv_name = search_between(query,'to', ';')
    print(f'Exporting table -> {table_name} to file {csv_name}')

def insert_into_table(query):
    table_name = search_between(query,'into', 'values')
    values = search_between(search_between(query,'values', ';'),'[', ']').split(',')
    print(f'will insert {values} into {table_name}')

def select_table(dic):
    # print(query)
    # project_args = ['select']
    # evaluate from

    print(dic)
    return
    dic['from'] = {

    }

    select_args = ['select', 'from', 'where', 'order by', 'top']
    # qp = {
    #     'project': query['select'],
    #     'from':  
    # }


    # for 
    columns = search_between(query,'select', 'from')
    # table_name = next_word(query, 'from')
    table_name = search_between(query,'from ', ' ')

    condition = search_between(query,'where ', ' ')
    
    order_by = search_between(query,'order by ', ' ')
    asc = 'asc' in search_between(query, order_by, ';') if order_by is not None else None
    top_k = search_between(query,'top ', ' ')

    print(f'select {columns,table_name,condition,order_by,asc,top_k}')

def lock_table(query):
    pass

def delete_table(query):
    pass

def update_table(query):
    pass

def create_database(query):
    pass

def save_database(query):
    pass

def drop_database(query):
    pass

def load_database(query):
    pass

def create_index(query):
    pass

def drop_index(query):
    pass

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
        dic[kw1] = search_between(query,kw1,kw2).strip()

    if action=='select':
        dic = evaluate_from_clause(dic)
    
    
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
            join_dic['mode'] = join_sent[jidx-1]
            join_dic['left'] = join_sent[jidx-2]
        else:
            join_dic['mode'] = 'inner'
            join_dic['left'] = join_sent[jidx-1]
        join_dic['right'] = join_sent[jidx+1]
        join_dic['on'] = ''.join(join_sent[join_sent.index('on')+1:])
        dic['from'] = {'join': join_dic}
        # pass
        
    return dic

    # kw_per_action = {'create table': create_table,
    #                  'drop table': drop_table,
    #                  'cast': cast_table,
    #                  'import': import_table,
    #                  'export': export_table,
    #                  'insert into': insert_into_table,
    #                  'select': select_table,
    #                  'lock table': lock_table,
    #                  'delete from': delete_table,
    #                  'update table': update_table,
    #                  'create database': create_database,
    #                  'drop database': drop_database,
    #                  'save database': save_database,
    #                  'load database': load_database,
    #                  'create index': create_index,
    #                  'drop index': drop_index
    #                  }

def interpret(query):
    kw_per_action = {'create table': ['create table'],
                     'drop table': ['drop table'],
                     'cast': ['cast', 'from', 'to'],
                     'import': ['import', 'from'],
                     'export': ['export', 'to'],
                     'insert into': ['insert into', 'values'],
                     'select': ['select', 'from', 'where', 'order by', 'top'],
                     'lock table': ['lock', 'mode'],
                     'delete from': ['delete from', 'where'],
                     'update table': ['update table', 'set', 'where'],
                     'create database': ['create database'],
                     'drop database': ['drop database'],
                     'save database': ['save database'],
                     'load database': ['load database'],
                     'create index': ['create index', 'on', 'using'],
                     'drop index': ['drop index']
                     }

    if query[-1]!=';':
        query+=';'

    for kw in kw_per_action.keys():
        if query.startswith(kw):
            action = kw

    return create_query_plan(query, kw_per_action[action], action)

    # pprint(dic, sort_dicts=False)

    #evaluate f

    #     dics.append(dic)
    
    # for i in range(0,len(dics)-1)[::-1]:
    #     for key in dics[i]:
    #         if dics[i][key] == f'_ph{i}_':
    #             dics[i][key] = dics[i+1]
    # # print('###')
    # print(kws[0])

    # return actions[action](dic)



if __name__ == "__main__":
    fname = os.getenv('SQL')
    for line in open(fname, 'r').read().splitlines():
        if line.startswith('--'): continue
        dic = interpret(line.lower())
        pprint(dic, sort_dicts=False)


