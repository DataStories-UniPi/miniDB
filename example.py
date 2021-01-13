from database import Database


# create db with name "example"
db = Database('example', load=False)
# create a single table named "myclass"
db.create_table('myclass', ['col1', 'col2', 'col3'], [str,str,int])
class Example:
# insert 5 rows
	index=1

	def imports(i):
		if i == 0:
			db.insert('myclass', ['A', '101', '500'])
			db.insert('myclass', ['C', '514', '10'])
			db.insert('myclass', ['F', '3128', '70'])
			db.insert('myclass', ['L', '100', '30'])
			db.insert('myclass', ['W', '120', '50'])
		else:
			i=1

		return i
	 
	imports(index)
	print (index)

