import shutil
import sys
import os
import pickle

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
sys.path.append('miniDB/tests')

from miniDB import table
from miniDB import btree
from miniDB.database import *
from miniDB.table import *

sys.modules['table'] = table
sys.modules['btree'] = btree


def test_1():
    '''
    Successful INLJ test between 2 tables. One of them has an index (instructor).
    '''
    instructor = Table(load=os.path.abspath(os.getcwd())+'/joinTests/joinTest1/instructorExample.pkl')
    advisor = Table(load=os.path.abspath(os.getcwd())+'/joinTests/joinTest1/advisorExample.pkl')
    joinedTable = Table(load=os.path.abspath(os.getcwd())+'/joinTests/joinTest1/instructorAdvisorGtTest.pkl')

    db = Database('smdb', load=True)
    join = db.join(mode = 'inlj', left_table = instructor, right_table = advisor, condition = 'id > i_id', save_as=None, return_object=True)

    join.show()
    joinedTable.show()


    assert join.__dict__['column_names'] == joinedTable.__dict__['column_names']
    assert join.__dict__['column_types'] == joinedTable.__dict__['column_types']
    for d in join.__dict__['data']:
        assert d in joinedTable.__dict__['data']

def test_2():
    '''
    Successful SMJ test between 2 tables.
    '''
    instructor = Table(load=os.path.abspath(os.getcwd())+'/joinTests/joinTest2/instructorExample.pkl')
    advisor = Table(load=os.path.abspath(os.getcwd())+'/joinTests/joinTest2/advisorExample.pkl')
    joinedTable = Table(load=os.path.abspath(os.getcwd())+'/joinTests/joinTest2/instructorAdvisorEqTest.pkl')

    db = Database('smdb', load=True)
    join = db.join(mode = 'smj', left_table = instructor, right_table = advisor, condition = 'id = i_id', save_as=None, return_object=True)

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
    takes = Table(load=os.path.abspath(os.getcwd())+'/joinTests/joinTest3/takesExample.pkl')
    teaches = Table(load=os.path.abspath(os.getcwd())+'/joinTests/joinTest3/teachesExample.pkl')
    joinedTable = Table(load=os.path.abspath(os.getcwd())+'/joinTests/joinTest3/takesTeachesFailInljTest.pkl')

    db = Database('smdb', load=True)
    join = db.join(mode = 'inlj', left_table = takes, right_table = teaches, condition = 'course_id = course_id', save_as=None, return_object=True)

    assert join.__dict__['column_names'] == joinedTable.__dict__['column_names']
    assert join.__dict__['column_types'] == joinedTable.__dict__['column_types']
    for d in join.__dict__['data']:
        assert d in joinedTable.__dict__['data']

def test_4():
    '''
    Unsuccessful SMJ test, no index of the given tables exists. Checks if the plan B of SMJ (inner join) works correctly.
    '''
    takes = Table(load=os.path.abspath(os.getcwd())+'/joinTests/joinTest4/takesExample.pkl')
    teaches = Table(load=os.path.abspath(os.getcwd())+'/joinTests/joinTest4/teachesExample.pkl')
    joinedTable = Table(load=os.path.abspath(os.getcwd())+'/joinTests/joinTest4/takesTeachesFailSmjTest.pkl')

    db = Database('smdb', load=True)
    join = db.join(mode = 'smj', left_table = takes, right_table = teaches, condition = 'course_id > course_id', save_as=None, return_object=True)

    if os.path.exists(os.getcwd() + '/miniDB/externalSortFolder'):
        shutil.rmtree(os.getcwd() + '/miniDB')

    assert join.__dict__['column_names'] == joinedTable.__dict__['column_names']
    assert join.__dict__['column_types'] == joinedTable.__dict__['column_types']
    for d in join.__dict__['data']:
        assert d in joinedTable.__dict__['data']


if __name__ == '__main__':
    takes = Table(load=os.path.abspath(os.getcwd())+'/joinTests/joinTest3/takesExample.pkl')
    takes.show()
    teaches = Table(load=os.path.abspath(os.getcwd())+'/joinTests/joinTest3/teachesExample.pkl')
    teaches.show()

    join = takes._inner_join(teaches, 'course_id > course_id')
    join.show()
    file = open(os.getcwd()+'/joinTests/joinTest4/takesTeachesFailSmjTest.pkl','wb+')
    pickle.dump(join, file)
    file.close()