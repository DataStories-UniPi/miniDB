import sys
import os
from pathlib import Path
sys.path.append(f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}')

from miniDB.database import *
from miniDB.table import *

from tests.helpFunctions import *

def runAllSqlFiles():
    '''
    Collects and executes all the testing SQL files to create changes in the Database
    that will later result in the test's pass or failure.
    '''
    # Collect all the sql files existing inside the folder that contains the sql tests
    allSqlTestFiles = list(Path("testSqls").rglob("*.sql"))

    for sql in allSqlTestFiles:
        resetAndTest(sql)


if __name__ == '__main__':
    db = Database('test', load=True)
    runAllSqlFiles()
    for table in list(db.tables.keys()):
        if table.startswith('_test_'):
            # For the test's name, we used the substring after '_test_', as the naming convention is '_test_NAMEOFTEST'.
            # This can be changed of course if the user sees fit.
            print(db.compare(table, table.replace('_test_',''), table.replace('_test_','')))