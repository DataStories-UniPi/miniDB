# region imports
import shutil
import sys
import os

sys.path.append(f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}')

from miniDB.database import *
from miniDB.table import *

from mdb import interpret, execute_dic, interpret_meta

# endregion

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

def test_2(sqlTest=f'{os.getcwd()}/joinTests/test2.sql'):
    '''
    Successful SMJ test between 2 tables.
    '''
    makeUniqueChanges(sqlTest)
    try:
        shutil.rmtree(f'{os.getcwd()}/miniDB')
    except:
        pass


# def test_3(sqlTest=None):
#     '''
#     Unsuccessful INLJ test, no index of the given tables exists. Checks if the failure of INLJ works correctly.
#     '''
#     makeUniqueChanges(sqlTest)
#     db = Database('test', load=True)

#     takes = Table(load=f'{os.getcwd()}/dbdata/test_db/takes.pkl')
#     teaches = Table(load=f'{os.getcwd()}/dbdata/test_db/teaches.pkl')
#     # The join is not actually needed
#     # join = Table(load=f'{os.getcwd()}/dbdata/test_db/joins.pkl')

#     with pytest.raises(Exception) as exc:
#         inlj = db.join(mode = 'inl', left_table = takes, right_table = teaches, condition = 'course_id = course_id', save_as=None, return_object=True)
    
#     assert 'Index-nested-loop join cannot be executed. Using inner join instead.\n' in str(exc.value)

# def test_4(sqlTest=None):
#     '''
#     Unsuccessful SMJ test, no index of the given tables exists. Checks if the plan B of SMJ (inner join) works correctly.
#     '''
#     makeUniqueChanges(sqlTest)
#     db = Database('test', load=True)

#     takes = Table(load=f'{os.getcwd()}/dbdata/test_db/takes.pkl')
#     teaches = Table(load=f'{os.getcwd()}/dbdata/test_db/teaches.pkl')
#     # The join is not actually needed
#     # join = Table(load=f'{os.getcwd()}/dbdata/test_db/joins.pkl')

#     with pytest.raises(Exception) as exc:
#         inlj = db.join(mode = 'sm', left_table = takes, right_table = teaches, condition = 'course_id > course_id', save_as=None, return_object=True)
    
#     assert "Sort-Merge Join is used when the condition operator is '='." in str(exc.value)

# # endregion