from database import Database
import sys

db = Database(sys.argv[1])
for name in db.tables.keys():
    if sys.argv[2]=='y' and name[:4]!='meta':
        continue
    db.show_table(name)
    pass
