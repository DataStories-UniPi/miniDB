from database import Database
import sys

db = Database(sys.argv[1])


for name in list(db.tables): 
    if sys.argv[2]=='meta' and name[:4]!='meta':
        continue
    db.show_table(name)
