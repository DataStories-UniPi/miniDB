from database import Database
# create db with name "example"
db = Database('example', load=False)
# create a single table named "myclass"
db.create_table('myclass', ['col1', 'col2', 'col3'], [str,str,int])
# insert 5 rows
db.insert('myclass', ['M', '101', '500'])
db.insert('myclass', ['A', '514', '10'])
db.insert('myclass', ['P', '3128', '70'])
db.insert('myclass', ['T', '100', '30'])
db.insert('myclass', ['W', '120', '50'])

