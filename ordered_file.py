from database import Database

db = Database('ofdb', load=False)

db.create_table('graduates', ['id', 'name', 'surname', 'department', 'grade'], [str,str,str,str,int], primary_key='id')

db.insert('graduates', ['B10101', 'Eleni', 'Mika','CS', '10'])
db.insert('graduates', ['A10250', 'Maria', 'Papadopoulou','EC', '10'])
db.insert('graduates', ['T15510', 'Dimitra', 'Gewrgouli','DS', '10'])
db.insert('graduates', ['P51515', 'Athanasia', 'Mosxouli', 'EC', '9'])
db.insert('graduates', ['R51515', 'Marios', 'Mpekas', 'CS', '9'])
db.insert('graduates', ['E55111', 'Kwstas', 'Paulou', 'DS', '8'])
db.insert('graduates', ['Y15151', 'Paulos', 'Mpratis', 'CS', '8'])
db.insert('graduates', ['E21021', 'Xristina', 'Ferentinos', 'MS', '8'])
db.insert('graduates', ['V45151', 'Giorgos', 'Skoulos', 'RLAW', '8'])
db.insert('graduates', ['W22515', 'Nikos', 'Menegakis', 'THEOL', '8'])
db.insert('graduates', ['B55151', 'Iasonas', 'Iwannou', 'PHYL', '7'])
db.insert('graduates', ['E51511', 'Katerina', 'Iwsifidou', 'EDU', '7'])
db.insert('graduates', ['N55151', 'Iliana', 'Iwakim', 'THEOL', '7'])
db.insert('graduates', ['Q55155', 'Stevi', 'Kasimath', 'HS', '7'])
db.insert('graduates', ['X55115', 'Petros', 'Mpikas', 'PS', '7'])
db.insert('graduates', ['J15151', 'Vasiliki', 'Giannakopoulou', 'RLAW', '7'])
db.insert('graduates', ['M51515', 'Aggelos', 'Karafwtis', 'EDU', '6'])
