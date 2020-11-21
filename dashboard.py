from database import Database
import sys, os
from time import sleep

db = Database(sys.argv[1])

while True:
    for name in list(db.tables): 
        if sys.argv[2]=='meta' and name[:4]!='meta':
            continue
        db.show_table(name)
