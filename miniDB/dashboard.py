import sys
import os

sys.path.append(f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/miniDB')

from database import Database


db = Database(sys.argv[1])


for name in list(db.tables): 
    if sys.argv[2]=='meta' and name[:4]!='meta':
        continue
    db.show_table(name)
