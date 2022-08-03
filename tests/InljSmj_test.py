# region imports
import shutil
import sys
import os
import pickle

# The following section of code is important! Do not remove without checking for errors.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
sys.path.append('miniDB/tests')

from miniDB import table
from miniDB import btree
from miniDB.database import *
from miniDB.table import *

sys.modules['table'] = table
sys.modules['btree'] = btree

# endregion

# region helpFunctions

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

# endregion

# region testFunctions

def test_1():
    '''
    Successful INLJ test between 2 tables. One of them has an index (instructor).
    '''
    instructor = csv_to_table('instructor', filename=os.getcwd()+'/joinTests/joinTest1/instructorExample.csv')
    advisor = csv_to_table('advisor', filename=os.getcwd()+'/joinTests/joinTest1/advisorExample.csv')
    joinedTable = csv_to_table('Join', filename=os.getcwd()+'/joinTests/joinTest1/instructorAdvisorGtTest.csv')
    
    db = Database('smdb', load=True)
    join = db.join(mode = 'inl', left_table = instructor, right_table = advisor, condition = 'id > i_id', save_as=None, return_object=True)

    assert join.__dict__['column_names'] == joinedTable.__dict__['column_names']
    assert join.__dict__['column_types'] == joinedTable.__dict__['column_types']
    for d in join.__dict__['data']:
        assert d in joinedTable.__dict__['data']

def test_2():
    '''
    Successful SMJ test between 2 tables.
    '''
    instructor = csv_to_table('instructor', filename=os.getcwd()+'/joinTests/joinTest2/instructorExample.csv')
    advisor = csv_to_table('advisor', filename=os.getcwd()+'/joinTests/joinTest2/advisorExample.csv')
    joinedTable = csv_to_table('Join', filename=os.getcwd()+'/joinTests/joinTest2/instructorAdvisorEqTest.csv')

    db = Database('smdb', load=True)
    join = db.join(mode = 'sm', left_table = instructor, right_table = advisor, condition = 'id = i_id', save_as=None, return_object=True)

    if os.path.exists(os.getcwd() + '/miniDB'):
            os.rmdir(os.getcwd() + '/miniDB')

    assert join.__dict__['column_names'] == joinedTable.__dict__['column_names']
    assert join.__dict__['column_types'] == joinedTable.__dict__['column_types']
    for d in join.__dict__['data']:
        assert d in joinedTable.__dict__['data']

def test_3():
    '''
    Unsuccessful INLJ test, no index of the given tables exists. Checks if the plan B of INLJ (inner join) works correctly.
    '''
    takes = csv_to_table('takes', filename=os.getcwd()+'/joinTests/joinTest3/takesExample.csv')
    teaches = csv_to_table('teaches', filename=os.getcwd()+'/joinTests/joinTest3/teachesExample.csv')
    joinedTable = csv_to_table('Join', filename=os.getcwd()+'/joinTests/joinTest3/takesTeachesFailInljTest.csv')

    db = Database('smdb', load=True)
    join = db.join(mode = 'inl', left_table = takes, right_table = teaches, condition = 'course_id = course_id', save_as=None, return_object=True)

    assert join.__dict__['column_names'] == joinedTable.__dict__['column_names']
    assert join.__dict__['column_types'] == joinedTable.__dict__['column_types']
    for d in join.__dict__['data']:
        assert d in joinedTable.__dict__['data']

def test_4():
    '''
    Unsuccessful SMJ test, no index of the given tables exists. Checks if the plan B of SMJ (inner join) works correctly.
    '''
    takes = csv_to_table('takes', filename=os.getcwd()+'/joinTests/joinTest4/takesExample.csv')
    teaches = csv_to_table('teaches', filename=os.getcwd()+'/joinTests/joinTest4/teachesExample.csv')
    joinedTable = csv_to_table('Join', filename=os.getcwd()+'/joinTests/joinTest4/takesTeachesFailSmjTest.csv')

    db = Database('smdb', load=True)
    join = db.join(mode = 'sm', left_table = takes, right_table = teaches, condition = 'course_id > course_id', save_as=None, return_object=True)

    if os.path.exists(os.getcwd() + '/miniDB/externalSortFolder'):
        shutil.rmtree(os.getcwd() + '/miniDB')

    assert join.__dict__['column_names'] == joinedTable.__dict__['column_names']
    assert join.__dict__['column_types'] == joinedTable.__dict__['column_types']
    for d in join.__dict__['data']:
        assert d in joinedTable.__dict__['data']

# endregion


if __name__ == '__main__':
    # takes = Table(load=os.path.abspath(os.getcwd())+'/joinTests/joinTest3/takesExample.pkl')
    # takes.show()
    # teaches = Table(load=os.path.abspath(os.getcwd())+'/joinTests/joinTest3/teachesExample.pkl')
    # teaches.show()

    '''Code used for creation of pkl files'''

    # join = takes._inner_join(teaches, 'course_id > course_id')
    # join.show()
    # file = open(os.getcwd()+'/joinTests/joinTest4/takesTeachesFailSmjTest.pkl','wb+')
    # pickle.dump(join, file)
    # file.close()

    '''Code used for creation of csv files'''

    # db = Database('smdb', load=True)
    # t1 = Table(load=os.path.abspath(os.getcwd())+'/joinTests/joinTest4/takesTeachesFailSmjTest.pkl')
    # table_to_csv(t1, 'takesTeachesFailSmjTest.csv', '/joinTests/joinTest4')

    '''Code used for creation of objects from csv files'''
    
    # db = Database('smdb', load=True)
    # t1 = csv_to_table('Join', filename=os.getcwd()+'/joinTests/joinTest1/instructorExample.csv')
    # print(t1.pk)
    # t1.show()