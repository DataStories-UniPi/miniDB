# region imports
import sys
import os

sys.path.append(f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}')

from miniDB.database import *
from miniDB.table import *

from mdb import interpret, execute_dic, interpret_meta

def resetAndTest(sqlTest, sqlDB=f'{os.getcwd()}/smallRelationsInsertFile.sql'):

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
