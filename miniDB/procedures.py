import table
def test1(table):
	print(table.column_names)
	table.order_by(table.column_names[0])
	table.show()    