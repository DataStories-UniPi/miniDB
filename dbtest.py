from database import Database

db = Database('testdb')
db.table_from_csv('students.csv', column_types=[str,int,str,int], primary_key='Student_id')
db.create_index('students', 'id1')
