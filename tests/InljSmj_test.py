# region imports
import shutil
import sys
import os
import pickle
import pytest

sys.path.append(f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}')

from miniDB.database import *
from miniDB.table import *

from mdb import interpret, execute_dic, interpret_meta, search_between

# endregion

'''
THESE ARE CURRENTLY NOT IN USE! SQL FILES ARE USED INSTEAD OF CSV FILES. MIGHT NEED TO BE DELETED
'''
def csv_to_table(table_name, filename):
    '''
    Creates table from CSV file.

    Args:
        table_name: string. The table object's name.
        filename: string. CSV filename. If not specified, filename's name will be used.
    '''
    file = open(filename, 'r')
    table = None

    lines = file.readlines()

    # Set important info like primary key, column types and column names first
    pk_idx = lines[0].strip('\n')
    colTypes = lines[1].strip('\n').split(',')
    colNames = lines[2].strip('\n').split(',')
    primary_key = colNames[int(pk_idx)] if pk_idx != '-1' else None

    table = Table(name=table_name, column_names=colNames, column_types=colTypes, primary_key=primary_key)

    # Add the table's data
    for line in lines[3:]:
        table._insert(line.strip('\n').split(','))
    table._update()
    
    return table

def table_to_csv(table_object, filename=None, directory=None):
    '''
    Transform table to CSV.

    Args:
        table_object: Table object. The Table object that will be represented in csv format.
        filename: string. Output CSV filename.
        directory: string. Output CSV parent directory and path.
    '''
    res = ''

    res += f'-1\n' if table_object.pk is None else f'{table_object.pk_idx}\n'
    res += str([str(t.__name__) for t in table_object.column_types])[1:-1].replace('\'', '').replace('"','').replace(' ','')+'\n'

    for row in [table_object.column_names] + table_object.data:
        res+=str(row)[1:-1].replace('\'', '').replace('"','').replace(' ','')+'\n'

    if filename is None:
        filename = f'{table_object._name}.csv'
    
    if directory is not None:
        filename = f'{directory}/{filename}'

    file = open(os.getcwd()+filename, 'w+')
    file.write(res)
    file.close()

# region helpFunctions

def makeUniqueChanges(sqlTest, sqlDB=f'{os.getcwd()}/smallRelationsInsertFile.sql'):
    db = Database('test', load=True)
    interpret_meta('cdb test;')

    hardResetTestDB(sqlDB)
    
    # Make the unique changes described in the corresponding sqlTest SQL file
    if sqlTest is not None:
        for line in open(sqlTest, 'r').read().splitlines():
            if line.startswith('--'): continue
            dic = interpret(line.lower())
            execute_dic(dic)

def hardResetTestDB(sqlDB = f'{os.getcwd()}/smallRelationsInsertFile.sql'):
    db = Database('test', load=True)
    interpret_meta('cdb test;')

    # Reset the tables of the database using the sqlDB SQL file provided
    for line in open(sqlDB, 'r').read().splitlines():
        if line.startswith('--'): continue
        dic = interpret(line.lower())
        execute_dic(dic)

    # Delete all the indexes created. This ensures the lack of conflicts between the tests
    for index in db.tables['meta_indexes'].column_by_name('index_name'):
        print(index)
        try:
            dic = interpret(f'drop index {index}')
            execute_dic(dic)
        except Exception as e:
            print(e)

# endregion

# region testFunctions

def test_1(sqlTest=f'{os.getcwd()}/joinTests/test1.sql'):
    '''
    Successful INLJ test between 2 tables. One of them has an index (instructor).
    '''
    makeUniqueChanges(sqlTest)
    db = Database('test', load=True)

    instructor = Table(load=f'{os.getcwd()}/dbdata/test_db/instructor.pkl')
    advisor = Table(load=f'{os.getcwd()}/dbdata/test_db/advisor.pkl')
    join = Table(load=f'{os.getcwd()}/dbdata/test_db/joins.pkl')

    inlj = db.join(mode = 'inl', left_table = instructor, right_table = advisor, condition = 'id > i_id', save_as=None, return_object=True)
    
    assert join.__dict__['column_names'] == inlj.__dict__['column_names']
    assert join.__dict__['column_types'] == inlj.__dict__['column_types']
    for d in join.__dict__['data']:
        assert d in inlj.__dict__['data']

def test_2(sqlTest=f'{os.getcwd()}/joinTests/test2.sql'):
    '''
    Successful SMJ test between 2 tables.
    '''
    makeUniqueChanges(sqlTest)
    db = Database('test', load=True)

    instructor = Table(load=f'{os.getcwd()}/dbdata/test_db/instructor.pkl')
    advisor = Table(load=f'{os.getcwd()}/dbdata/test_db/advisor.pkl')
    join = Table(load=f'{os.getcwd()}/dbdata/test_db/joins.pkl')

    smj = db.join(mode = 'sm', left_table = instructor, right_table = advisor, condition = 'id = i_id', save_as=None, return_object=True)
    
    assert join.__dict__['column_names'] == smj.__dict__['column_names']
    assert join.__dict__['column_types'] == smj.__dict__['column_types']
    for d in join.__dict__['data']:
        assert d in smj.__dict__['data']
    try:
        shutil.rmtree(f'{os.getcwd()}/miniDB')
    except:
        pass

def test_3(sqlTest=None):
    '''
    Unsuccessful INLJ test, no index of the given tables exists. Checks if the failure of INLJ works correctly.
    '''
    makeUniqueChanges(sqlTest)
    db = Database('test', load=True)

    takes = Table(load=f'{os.getcwd()}/dbdata/test_db/takes.pkl')
    teaches = Table(load=f'{os.getcwd()}/dbdata/test_db/teaches.pkl')
    # The join is not actually needed
    # join = Table(load=f'{os.getcwd()}/dbdata/test_db/joins.pkl')

    with pytest.raises(Exception) as exc:
        inlj = db.join(mode = 'inl', left_table = takes, right_table = teaches, condition = 'course_id = course_id', save_as=None, return_object=True)
    
    assert 'Index-nested-loop join cannot be executed. Using inner join instead.\n' in str(exc.value)

def test_4(sqlTest=None):
    '''
    Unsuccessful SMJ test, no index of the given tables exists. Checks if the plan B of SMJ (inner join) works correctly.
    '''
    makeUniqueChanges(sqlTest)
    db = Database('test', load=True)

    takes = Table(load=f'{os.getcwd()}/dbdata/test_db/takes.pkl')
    teaches = Table(load=f'{os.getcwd()}/dbdata/test_db/teaches.pkl')
    # The join is not actually needed
    # join = Table(load=f'{os.getcwd()}/dbdata/test_db/joins.pkl')

    with pytest.raises(Exception) as exc:
        inlj = db.join(mode = 'sm', left_table = takes, right_table = teaches, condition = 'course_id > course_id', save_as=None, return_object=True)
    
    assert "Sort-Merge Join is used when the condition operator is '='." in str(exc.value)

# endregion