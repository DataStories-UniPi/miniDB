from database import Database
import sys

db = Database(sys.argv[1])
for name in db.tables.keys():
    db.show_table(name)
    print('\n')
    pass
