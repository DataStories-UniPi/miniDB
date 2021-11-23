import os
import re

def search_between(s, first, last):

    # return re.search(fr'{a}(.*){b}', line).group(1)
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        if '(' in s[start:end]:
            return s[start:s.index( ')', start )+1]
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

def select_table(query):
    print(query)
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

def analyze(query):
    actions = {'create table ': create_table,
               'drop table ': drop_table,
               'cast ': cast_table,
               'import ': import_table,
               'export ': export_table,
               'insert into ': insert_into_table,
               'select ': select_table,
               'lock table ': lock_table,
               'delete ': delete_table,
               'update ': update_table,
               'create database ': create_database,
               'drop database ': drop_database,
               'save database ': save_database,
               'load database ': load_database,
               'create index ': create_index,
               'drop index ': drop_index
               }

    acts = [key for key in actions.keys() if key in query]
    for act in acts:
        print(act)
        actions[act](query)


if __name__ == "__main__":
    fname = os.getenv('SQL')
    for line in open(fname, 'r').read().splitlines():
        if line.startswith('--'): continue
        analyze(line.lower())

