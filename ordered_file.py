from database import Database

# create db with name "ofdb"
db = Database('ofdb', load=False)

class Graduates:
	def __init__(self):
		self.data = {} # data is the list of rows that included in the table.
			

	# create a table named graduates
	db.create_table('graduates', ['id', 'name', 'surname', 'department', 'grade'], [str,str,str,str,int], primary_key='id')
	
	def ins_data(self):
		self.data = {
			0 : ['B10101', 'Eleni', 'Mika','CS', '10'],
			1 : ['A10250', 'Maria', 'Papadopoulou','EC', '10'],
			2 : ['T15510', 'Dimitra', 'Gewrgouli','DS', '10'],
			3 : ['P51515', 'Athanasia', 'Mosxouli', 'EC', '9'],
			4 : ['R51515', 'Marios', 'Mpekas', 'CS', '9'],
			5 : ['E55111', 'Kwstas', 'Paulou', 'DS', '8'],
			6 : ['Y15151', 'Paulos', 'Mpratis', 'CS', '8'],
			7 : ['E21021', 'Xristina', 'Ferentinos', 'MS', '8'],
			8 : ['V45151', 'Giorgos', 'Skoulos', 'RLAW', '8'],
			9 : ['W22515', 'Nikos', 'Menegakis', 'THEOL', '8'],
			10 : ['B55151', 'Iasonas', 'Iwannou', 'PHYL', '7'],
			11 : ['E51511', 'Katerina', 'Iwsifidou', 'EDU', '7'],
			12 : ['N55151', 'Iliana', 'Iwakim', 'THEOL', '7'],
			13 : ['Q55155', 'Stevi', 'Kasimath', 'HS', '7'],
			14 : ['X55115', 'Petros', 'Mpikas', 'PS', '7'],
			15 : ['J15151', 'Vasiliki', 'Giannakopoulou', 'RLAW', '7'],
			16 : ['M51515', 'Aggelos', 'Karafwtis', 'EDU', '6']
		}
		# insert data
		for i in range(17) : 
			db.insert('graduates', self.data[i])
gr = Graduates()
gr.ins_data()

