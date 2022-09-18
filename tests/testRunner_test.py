import shutil
import sys
import os
import pytest
from pathlib import Path
sys.path.append(f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}')

from miniDB.database import *
from miniDB.table import *

from tests.helpFunctions import *


def test():
    # Collect all the sql files existing inside the folder that contains the sql tests
    allSqlTestFiles = list(Path("testSqls").rglob("*.sql"))

    successful_SQLs = [sql for sql in allSqlTestFiles if '/successful/' in str(sql)]
    failing_SQLs = [sql for sql in allSqlTestFiles if '/failing/' in str(sql)]

    # Asserting no exceptions will be raised
    for sql in successful_SQLs:
        resetAndTest(sql)

    # Asserting a custom exception used for failing scenarios will be raised
    for sql in failing_SQLs:
        with pytest.raises(Exception) as exc:
            resetAndTest(sql)
        assert exc.type.__name__ == 'CustomFailException'

    # Deleting any leftover unused folders created by functions run by tests
    try:
        shutil.rmtree(f'{os.getcwd()}/miniDB')
    except:
        pass