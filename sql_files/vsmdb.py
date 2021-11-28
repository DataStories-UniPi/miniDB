#run using:
#python3 -i -m src.insert.vsmdb

import os, sys
# currentdir = os.path.dirname(os.path.realpath(__file__))
# parentdir = os.path.dirname(currentdir)
sys.path.append('../miniDB')

from database import Database
# create db with name "smdb"
db = Database('vsmdb', load=False)
# create a single table named "classroom"
db.create_table('classroom', 'building, room_number, capacity', 'str,str,int')
# insert 5 rows
db.insert_into('classroom', 'Packard,101,500')
db.insert_into('classroom', 'Painter,514,10')
db.insert_into('classroom', 'Taylor,3128,70')
db.insert_into('classroom', 'Watson,100,30')
db.insert_into('classroom', 'Watson,120,50')

print(db.tables['classroom'].building)
