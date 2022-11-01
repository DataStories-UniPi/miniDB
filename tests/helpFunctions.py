# region imports
import sys
import os

sys.path.append(f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}')

from miniDB.database import *
from miniDB.table import *

from mdb import interpret, execute_dic, interpret_meta

def resetAndTest(db, sqlTest, sqlDB=f'{os.getcwd()}/smallRelationsInsertFile.sql'):
    '''
    Resets the default tables of the testing database, deletes all the indexes and executes the given SQL code.

    Args:
        db: Database. The testing database.
        sqlTest: string. Path to the SQL file that contains the changes that should be made to the testing DB.
        sqlDB: string. Blueprint of the testing DB so that it is reset easily and quickly.
    '''
    # Reset the default tables of the database using the sqlDB SQL file provided
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

    # Make the unique changes described in the corresponding sqlTest SQL file
    if sqlTest is not None:
        for line in open(sqlTest, 'r').read().splitlines():
            if line.startswith('--'): continue
            dic = interpret(line.lower())
            execute_dic(dic)
        
    return db