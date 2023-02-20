#First import the necessary modules
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
art = '''
             _         _  _____   ____  
            (_)       (_)|  __ \ |  _ \     
  _ __ ___   _  _ __   _ | |  | || |_) |
 | '_ ` _ \ | || '_ \ | || |  | ||  _ < 
 | | | | | || || | | || || |__| || |_) |
 |_| |_| |_||_||_| |_||_||_____/ |____/   2022                              
'''   
def anazitisi_anamesa(s, proto, teleutaio): #Search the substring between arxh and telos.
    try:
        arxh = s.index( proto ) + len(proto)
        telos = s.index( teleutaio, start )
    except:
        return
    return s[arxh:telos].strip()

def entos_parenthesis(qsplit, ind):
    '''
    Split string on space and return whether the item in index 'ind' is inside a parentheses
    '''
    return qsplit[:ind].count('(')>qsplit[:ind].count(')')


def dimiourgia_protou_planou(frasi, lexeis_kleidia, energeia): #Ena plano ektelesis mias sql entolis
    dic = {val: None for val in lexeis_kleidia if val!=';'}
    ql = [val for val in frasi.split(' ') if val !='']
    keyword_in_query = []
    keyword_positions = []
    metritis=0
    while metritis<len(ql):
        if entos_parenthesis(ql, metritis): 
            metritis+=1
            continue
        if ql[metritis] in lexeis_kleidia:
            keyword_in_query.append(ql[metritis])
            keyword_positions.append(metritis)
        elif metritis!=len(ql)-1 and f'{ql[metritis]} {ql[metritis+1]}' in lexeis_kleidia:
            keyword_in_query.append(f'{ql[metritis]} {ql[metritis+1]}')
            ql[metritis] = ql[metritis]+' '+ql[metritis+1]
            ql.pop(metritis+1)
            keyword_positions.append(metritis)
        metritis+=1
    for metritis in range(len(keyword_in_query)-1):
        dic[keyword_in_query[metritis]] = ' '.join(ql[keyword_positions[metritis]+1:keyword_positions[metritis+1]])
    if energeia == 'create view':
        dic['as'] = diermineia(dic['as'])
    if energeia=='select':
        dic = poio_einai_from(dic)
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
    if energeia=='create table':
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
    if energeia=='import': 
        dic = {'import table' if key=='import' else key: val for key, val in dic.items()}
    if energeia=='insert into':
        if dic['values'][0] == '(' and dic['values'][-1] == ')':
            dic['values'] = dic['values'][1:-1]
        else:
            raise ValueError('Syntaktiko lathos. Prospathiste ksana.')
    if energeia=='unlock table':
        if dic['force'] is not None:
            dic['force'] = True
        else:
            dic['force'] = False
    if energeia=='create index':
        split_con=dic[keyword_in_query[1]].split() 
        split_con.remove("(")
        split_con.remove(")")
        dic['create index'] = dic[keyword_in_query[0]]
        dic['on'] = split_con[0]
        dic['column'] = split_con[1]
        dic['using'] = dic[keyword_in_query[2]]
    return dic

def poio_einai_from(lexikon):
    '''
    Evaluate the part of the query (argument or subquery) that is supplied as the 'from' argument
    '''
    enoseis = ['inner', 'left', 'right', 'full', 'sm', 'inl']
    diaxorise_from = lexikon['from'].split(' ')
    if diaxorise_from[0] == '(' and diaxorise_from[-1] == ')':
        tmima_tou_query = ' '.join(diaxorise_from[1:-1])
        lexikon['from'] = diermineia(tmima_tou_query)
    join_index = [i for i,word in enumerate(diaxorise_from) if word=='join' and not entos_parenthesis(diaxorise_from,i)]
    on_index = [i for i,word in enumerate(diaxorise_from) if word=='on' and not entos_parenthesis(diaxorise_from,i)]
    if join_index:
        join_index = join_index[0]
        on_index = on_index[0]
        enose_lexiko = {}
        if diaxorise_from[join_index-1] in enoseis:
            enose_lexiko['join'] = diaxorise_from[join_index-1]
            enose_lexiko['left'] = ' '.join(diaxorise_from[:join_index-1])
        else:
            enose_lexiko['join'] = 'inner'
            enose_lexiko['left'] = ' '.join(diaxorise_from[:join_index])
        enose_lexiko['right'] = ' '.join(diaxorise_from[join_index+1:on_index])
        enose_lexiko['on'] = ''.join(diaxorise_from[on_index+1:])
        if enose_lexiko['left'].startswith('(') and enose_lexiko['left'].endswith(')'):
            enose_lexiko['left'] = diermineia(enose_lexiko['left'][1:-1].strip())
        if enose_lexiko['right'].startswith('(') and enose_lexiko['right'].endswith(')'):
            enose_lexiko['right'] = diermineia(enose_lexiko['right'][1:-1].strip())
        lexikon['from'] = enose_lexiko
    return lexikon

def diermineia(query):
    '''
    Interprets the query.
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
                     'create index': ['create index', 'on', 'column', 'using'],
                     'drop index': ['drop index'],
                     'create view' : ['create view', 'as']
                     }
    if query[-1]!=';':
        query+=';'
    query = query.replace("(", " ( ").replace(")", " ) ").replace(";", " ;").strip()
    for kw in kw_per_action.keys():
        if query.startswith(kw):
            energeia = kw
    return dimiourgia_protou_planou(query, kw_per_action[energeia]+[';'], energeia)

def ektelesi_lexikou(dic):
    '''
    Execute the given dictionary
    '''
    for key in dic.keys():
        if isinstance(dic[key],dict):
            dic[key] = ektelesi_lexikou(dic[key])
    energeia = list(dic.keys())[0].replace(' ','_')
    return getattr(db, energeia)(*dic.values())

def diermineia_meta(entoli):
    """
    Diermineia entolon meta.
    """
    energeia = entoli.split(' ')[0].removesuffix(';')
    db_name = db._name if anazitisi_anamesa(entoli, energeia,';')=='' else anazitisi_anamesa(entoli, energeia,';')
    verbose = True
    if energeia == 'cdb' and ' -noverb' in db_name:
        db_name = db_name.replace(' -noverb','')
        verbose = False
    def katalogos_vaseon(db_name):
        [print(fold.removesuffix('_db')) for fold in os.listdir('dbdata')]
    def katalogos_pinakon(db_name):
        [print(pklf.removesuffix('.pkl')) for pklf in os.listdir(f'dbdata/{db_name}_db') if pklf.endswith('.pkl')\
            and not pklf.startswith('meta')]
    def allagi_sti_vasi(db_name):
        global db
        db = Database(db_name, load=True, verbose=verbose)
    def afairesi_tis_vasis(db_name):
        shutil.rmtree(f'dbdata/{db_name}_db')
    lista_entolon = {
        'lsdb': katalogos_vaseon,
        'lstb': katalogos_pinakon,
        'cdb': allagi_sti_vasi,
        'rmdb': afairesi_tis_vasis,
    }

    lista_entolon[action](db_name)

if __name__ == "__main__":
    fname = os.getenv('SQL')
    dbname = os.getenv('DB')
    db = Database(dbname, load=True)
    if fname is not None:
        for line in open(fname, 'r').read().splitlines():
            if line.startswith('--'): continue
            if line.startswith('explain'):
                dic = diermineia(line.removeprefix('explain '))
                pprint(dic, sort_dicts=False)
            else :
                dic = diermineia(line.lower())
                result = ektelesi_lexikou(dic)
                if isinstance(result,Table):
                    result.show()
    from prompt_toolkit import PromptSession
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
    print(art)
    synedria = PromptSession(history=FileHistory('.inp_history'))
    while 1:
        try:
            line = synedria.prompt(f'({db._name})> ', auto_suggest=AutoSuggestFromHistory()).lower()
            if line[-1]!=';':
                line+=';'
        except (KeyboardInterrupt, EOFError):
            print('Telos.')
            break
        try:
            if line=='exit':
                break
            if line.split(' ')[0].removesuffix(';') in ['lsdb', 'lstb', 'cdb', 'rmdb']:
                diermineia_meta(line)
            elif line.startswith('explain'):
                dic = diermineia(line.removeprefix('explain '))
                pprint(dic, sort_dicts=False)
            else:
                dic = diermineia(line)
                result = ektelesi_lexikou(dic)
                if isinstance(result,Table):
                    result.show()
        except Exception:
            print(traceback.format_exc())

def dimiourgia_deuterou_planou(frasi, lexeis_kleidia, energeia): #Etero plano gia tin ektelesi mias sql entolis: σθ1∧θ2(E) = σθ1(σθ2(E))
    dic = {val: None for val in lexeis_kleidia if val!=';'}
    ql = [val for val in frasi.split(' ') if val !='']
    keyword_in_query = []
    keyword_positions = []
    metritis=0
    while metritis<len(ql):
        if entos_parenthesis(ql, metritis): 
            metritis+=1
            continue
        if ql[metritis] in lexeis_kleidia:
            keyword_in_query.append(ql[metritis])
            keyword_positions.append(metritis)
        elif metritis!=len(ql)-1 and f'{ql[metritis]} {ql[metritis+1]}' in lexeis_kleidia:
            keyword_in_query.append(f'{ql[metritis]} {ql[metritis+1]}')
            ql[metritis] = ql[metritis]+' '+ql[metritis+1]
            ql.pop(metritis+1)
            keyword_positions.append(metritis)
        metritis+=1
    for metritis in range(len(keyword_in_query)-1):
        dic[keyword_in_query[metritis]] = ' '.join(ql[keyword_positions[metritis]+1:keyword_positions[metritis+1]])
    if energeia=='select':
        dic = poio_einai_from_deuterou_planou(dic) 
        if "and" in dic[keyword_in_query[2]]:    
            split_con=dic[keyword_in_query[2]].split() 
            if ("()" in split_con) or (")" in split_con):
                split_con.remove("(")
                split_con.remove(")")
                split_con.remove("(")
                split_con.remove(")")
            query_s1 = ''.join(split_con[0]) 
            query_s2 = ''.join(split_con[2])    
            query_s2_E= diermineia("select" + dic[keyword_in_query[0]] + "from" + dic[keyword_in_query[1]] + "where" + query_s2 ) #stelnoume gia ektelesi to kommati s2(E)
            query_se_e= diermineia("select" + dic[keyword_in_query[0]] + "from" + query_s2_E + "where" +  query_s1) #stelnoume gia ektelesi thn teliki synthiki
            dic['where'] = query_se_e #to apotelesmaa twn 2 praksewn to vazoume to dic['where'] 
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
        return dic    


def poio_einai_from_deuterou_planou(dic): #Find the 'from' argument
    enoseis = ['inner', 'left', 'right', 'full', 'sm', 'inl']
    diaxorise_from = dic['from'].split(' ')
    if diaxorise_from[0] == '(' and diaxorise_from[-1] == ')':
        subquery = ' '.join(diaxorise_from[1:-1])
        dic['from'] = diermineia(subquery)
    join_index = [i for i,word in enumerate(diaxorise_from) if word=='join' and not entos_parenthesis(diaxorise_from,i)]
    on_index = [i for i,word in enumerate(diaxorise_from) if word=='on' and not entos_parenthesis(diaxorise_from,i)]
    if join_index:
        join_index = join_index[0]
        on_index = on_index[0]
        enose_lexiko = {}
        if diaxorise_from[join_index-1] in join_types:
            enose_lexiko['join'] = diaxorise_from[join_index-1]
            enose_lexiko['right'] = ' '.join(diaxorise_from[:join_index-1])
        else:
            enose_lexiko['join'] = 'inner'
            enose_lexiko['right'] = ' '.join(diaxorise_from[:join_index])
        enose_lexiko['left'] = ' '.join(diaxorise_from[join_index+1:on_index])
        enose_lexiko['on'] = ''.join(diaxorise_from[on_index+1:])
        if enose_lexiko['right'].startswith('(') and enose_lexiko['right'].endswith(')'):
            enose_lexiko['right'] = diermineia(enose_lexiko['right'][1:-1].strip())
        if enose_lexiko['left'].startswith('(') and enose_lexiko['left'].endswith(')'):
            enose_lexiko['left'] = diermineia(enose_lexiko['left'][1:-1].strip())
        dic['from'] = enose_lexiko
    return dic
